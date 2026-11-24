import os
import math
import time
import threading
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI
from dotenv import load_dotenv
from tronpy import Tron
from tronpy.providers import HTTPProvider
from tronpy.keys import PrivateKey

load_dotenv()

# --- CONFIGURACIÓN ---
TRON_ADDRESS = os.getenv("TRON_PUBLIC_ADDRESS", "").strip()
RAW_PRIV_KEY = os.getenv("TRON_PRIVATE_KEY", "").replace("0x", "").strip()
PRIV_KEY = PrivateKey(bytes.fromhex(RAW_PRIV_KEY))
VAULT_ADDRESS = os.getenv("ARBITRUM_VAULT_ADDRESS", "").strip()
CHANGENOW_API_KEY = os.getenv("CHANGENOW_API_KEY", "").strip()
TRONGRID_API_KEY = os.getenv("TRONGRID_API_KEY", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

MIN_TRX_GAS = float(os.getenv("MIN_TRX_GAS", "35.0"))
USDT_PARA_SWAP_GAS = float(os.getenv("USDT_PARA_SWAP_GAS", "10.0"))
MONTO_VERIFICACION_USDT = float(os.getenv("MONTO_VERIFICACION_USDT", "10.0"))

# Contratos Tron Mainnet
USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
WTRX_CONTRACT = "TNUC9Qb1rRpS5CbWLmNMxXBjyFoydXjWFR"
SUNSWAP_ROUTER = "TKzxdSv2FZKQrEqkKVgp5DcwEXBEKMg2Ax"

provider = HTTPProvider(api_key=TRONGRID_API_KEY) if TRONGRID_API_KEY else HTTPProvider()
client = Tron(provider=provider)

app = FastAPI(title="Recepcionista Tron - Neon")
lock_ciclo = threading.Lock()


def obtener_saldos():
    saldo_trx = float(client.get_account_balance(TRON_ADDRESS))
    contrato_usdt = client.get_contract(USDT_CONTRACT)
    saldo_usdt = contrato_usdt.functions.balanceOf(TRON_ADDRESS) / 1_000_000
    return saldo_trx, saldo_usdt


def asegurar_gas_trx() -> bool:
    saldo_trx, saldo_usdt = obtener_saldos()
    if saldo_trx >= MIN_TRX_GAS:
        return True

    if saldo_usdt < (USDT_PARA_SWAP_GAS + 2.0):
        print(f"⚠️ TRX bajo ({saldo_trx:.2f} TRX) y USDT insuficiente para auto-swap.")
        return False

    print(f"⛽ Gas bajo ({saldo_trx:.2f} TRX). Cambiando {USDT_PARA_SWAP_GAS} USDT -> TRX en SunSwap...")
    monto_in = int(USDT_PARA_SWAP_GAS * 1_000_000)
    contrato_usdt = client.get_contract(USDT_CONTRACT)
    router = client.get_contract(SUNSWAP_ROUTER)

    allowance = contrato_usdt.functions.allowance(TRON_ADDRESS, SUNSWAP_ROUTER)
    if allowance < monto_in:
        tx_approve = (
            contrato_usdt.functions.approve(SUNSWAP_ROUTER, 10**15)
            .with_owner(TRON_ADDRESS)
            .fee_limit(100_000_000)
            .build()
            .sign(PRIV_KEY)
        )
        tx_approve.broadcast().wait()
        time.sleep(3)

    deadline = int(time.time()) + 300
    tx_swap = (
        router.functions.swapExactTokensForETH(
            monto_in,
            1,
            [USDT_CONTRACT, WTRX_CONTRACT],
            TRON_ADDRESS,
            deadline
        )
        .with_owner(TRON_ADDRESS)
        .fee_limit(150_000_000)
        .build()
        .sign(PRIV_KEY)
    )
    res = tx_swap.broadcast().wait()
    print(f"✅ Gas TRX repuesto en SunSwap. TxID: {res.get('id')}")
    return True


def enviar_usdt(destino: str, monto_usdt: float) -> str:
    monto_formateado = int(round(monto_usdt * 1_000_000))
    contrato = client.get_contract(USDT_CONTRACT)
    txn = (
        contrato.functions.transfer(destino, monto_formateado)
        .with_owner(TRON_ADDRESS)
        .fee_limit(150_000_000)
        .build()
        .sign(PRIV_KEY)
    )
    res = txn.broadcast()
    txid = res.get("txid")
    if not txid:
        raise RuntimeError(f"Error al transmitir USDT: {res}")
    return txid


def obtener_historial_trongrid() -> dict:
    url = f"https://api.trongrid.io/v1/accounts/{TRON_ADDRESS}/transactions/trc20"
    headers = {"TRON-PRO-API-KEY": TRONGRID_API_KEY} if TRONGRID_API_KEY else {}
    params = {
        "only_to": "true",
        "only_confirmed": "true",
        "contract_address": USDT_CONTRACT,
        "limit": 100
    }
    resp = requests.get(url, params=params, headers=headers, timeout=15)
    resp.raise_for_status()
    datos = resp.json().get("data", [])
    return {tx["transaction_id"].strip().lower(): tx for tx in datos}


def conciliar_neon(conn, txs_red: dict):
    if not txs_red:
        return

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # 1. Aprobar recargas pendientes en 'depositos'
        cur.execute("""
            SELECT id, monto_usdt, tx_hash_tron
            FROM depositos
            WHERE estado = 'PENDIENTE'
              AND tx_hash_tron IS NOT NULL
              AND TRIM(tx_hash_tron) <> ''
        """)
        for dep in cur.fetchall():
            hash_db = dep["tx_hash_tron"].strip().lower()
            tx = txs_red.get(hash_db)
            if not tx:
                continue

            monto_recibido = int(tx["value"]) / 1_000_000
            if monto_recibido >= float(dep["monto_usdt"]):
                cur.execute("SELECT aprobar_deposito(%s);", (dep["tx_hash_tron"].strip(),))
                conn.commit()
                print(f"✅ Recarga aprobada | TxID: {hash_db} | {monto_recibido} USDT")

        # 2. Aprobar verificaciones de cuenta en 'users'
        cur.execute("""
            SELECT id, username, tx_hash_verificacion
            FROM users
            WHERE cuenta_verificada = FALSE
              AND tx_hash_verificacion IS NOT NULL
              AND TRIM(tx_hash_verificacion) <> ''
        """)
        for usr in cur.fetchall():
            hash_ver = usr["tx_hash_verificacion"].strip().lower()
            tx = txs_red.get(hash_ver)
            if not tx:
                continue

            monto_recibido = int(tx["value"]) / 1_000_000
            if monto_recibido >= MONTO_VERIFICACION_USDT:
                cur.execute(
                    "SELECT pagar_y_activar_verificacion(%s::uuid, 'usdt_trc20', %s);",
                    (str(usr["id"]), usr["tx_hash_verificacion"].strip())
                )
                conn.commit()
                print(f"✅ Cuenta verificada: {usr['username']} | TxID: {hash_ver}")


def obtener_minimo_changenow() -> float:
    url = "https://api.changenow.io/v2/exchange/min-amount"
    headers = {"x-changenow-api-key": CHANGENOW_API_KEY}
    params = {
        "fromCurrency": "usdt",
        "toCurrency": "eth",
        "fromNetwork": "trx",
        "toNetwork": "arb"
    }
    res = requests.get(url, params=params, headers=headers, timeout=10)
    res.raise_for_status()
    return float(res.json().get("minAmount", 15.0))


def crear_orden_changenow(monto: float) -> str:
    url = "https://api.changenow.io/v2/exchange"
    headers = {"x-changenow-api-key": CHANGENOW_API_KEY}
    payload = {
        "fromCurrency": "usdt",
        "toCurrency": "eth",
        "fromNetwork": "trx",
        "toNetwork": "arb",
        "fromAmount": monto,
        "address": VAULT_ADDRESS
    }
    res = requests.post(url, json=payload, headers=headers, timeout=15)
    res.raise_for_status()
    return res.json().get("payinAddress")


def puentear_a_arbitrum():
    _, saldo_usdt = obtener_saldos()
    if saldo_usdt <= 0:
        return {"puenteado": False, "motivo": "Saldo USDT en 0"}

    minimo_cn = obtener_minimo_changenow()
    if saldo_usdt < minimo_cn:
        return {
            "puenteado": False,
            "motivo": f"Saldo ({saldo_usdt:.2f} USDT) menor al mínimo ({minimo_cn} USDT)"
        }

    if not asegurar_gas_trx():
        return {"puenteado": False, "motivo": "TRX insuficiente para operar"}

    _, saldo_usdt_actual = obtener_saldos()
    monto_enviar = math.floor(saldo_usdt_actual * 100) / 100

    if monto_enviar < minimo_cn:
        return {
            "puenteado": False,
            "motivo": f"Saldo post-gas ({monto_enviar} USDT) menor al mínimo ({minimo_cn} USDT)"
        }

    direccion_exchange = crear_orden_changenow(monto_enviar)
    txid = enviar_usdt(direccion_exchange, monto_enviar)
    print(f"🌉 Enviado a Arbitrum: {monto_enviar} USDT -> {direccion_exchange} | TxID: {txid}")
    return {"puenteado": True, "monto": monto_enviar, "txid": txid}


@app.get("/")
def health():
    return {"status": "online", "service": "recepcionista-tron"}


@app.api_route("/tick", methods=["GET", "POST"])
def ejecutar_tick():
    if not lock_ciclo.acquire(blocking=False):
        return {"status": "busy", "message": "Ciclo en ejecución"}

    try:
        txs_red = obtener_historial_trongrid()
        with psycopg2.connect(DATABASE_URL) as conn:
            conciliar_neon(conn, txs_red)

        resultado_puente = puentear_a_arbitrum()
        saldo_trx, saldo_usdt = obtener_saldos()

        return {
            "status": "ok",
            "saldo_trx": round(saldo_trx, 2),
            "saldo_usdt": round(saldo_usdt, 2),
            "puente_arbitrum": resultado_puente
        }
    except Exception as e:
        print(f"❌ Error en /tick: {e}")
        return {"status": "error", "detail": str(e)}
    finally:
        lock_ciclo.release()