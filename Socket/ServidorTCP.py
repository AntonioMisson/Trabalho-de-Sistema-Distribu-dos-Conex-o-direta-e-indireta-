import socket
import threading
import json
import time
import os
import tempfile

HOST = "127.0.0.1"
PORT = 5000
DB_FILE = "Socket/Mensagem.json"

# ------------------------------------------------------------
# Configs
# ------------------------------------------------------------
MAX_MSG_SIZE = 16 * 1024  

# Lock para proteger acesso concorrente ao db
db_lock = threading.Lock()

# ------------------------------------------------------------
# Persistência (com atomic write e garantia de chaves)
# ------------------------------------------------------------

def load_db():
    base = {"vehicles": {}, "orders": {}}

    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, dict):
                    raise ValueError("Formato inválido do DB (esperado dict).")
                # garantir chaves
                if "vehicles" not in data or not isinstance(data.get("vehicles"), dict):
                    data["vehicles"] = {}
                if "orders" not in data or not isinstance(data.get("orders"), dict):
                    data["orders"] = {}
                return data
        except Exception as e:
            print(f"[ERRO] {DB_FILE} inválido/corrompido: {e}. Criando novo...")
    return base

def save_db_atomic(local_db):
    """
    Salva de forma atômica: escreve em arquivo temporário e substitui.
    Recebe uma cópia/visão do db (para evitar segurar o lock por muito tempo).
    """
    try:
        dirpath = os.path.dirname(os.path.abspath(DB_FILE)) or "."
        fd, tmpname = tempfile.mkstemp(dir=dirpath, prefix="msgdb_", suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as tmpf:
            json.dump(local_db, tmpf, indent=4, ensure_ascii=False)
            tmpf.flush()
            os.fsync(tmpf.fileno())
        os.replace(tmpname, DB_FILE)
    except Exception as e:
        print(f"[ERRO] falha ao salvar DB: {e}")

# carregamento inicial
db = load_db()

# ------------------------------------------------------------
# Utilitários
# ------------------------------------------------------------

def now():
    return time.strftime("%Y-%m-%d %H:%M:%S")

def log(msg):
    print(f"[{now()}] {msg}")

# ------------------------------------------------------------
# Validação simples de campos e tipos
# ------------------------------------------------------------
def validate_fields(obj, required_fields):
    for field in required_fields:
        if field not in obj:
            return False, f"Campo obrigatório ausente: {field}"
    return True, ""

def validate_lat_lon(lat, lon):
    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except Exception:
        return False, "lat/lon devem ser numéricos"
    if not (-90.0 <= lat_f <= 90.0 and -180.0 <= lon_f <= 180.0):
        return False, "lat ou lon fora do intervalo válido"
    return True, ""

# ------------------------------------------------------------
# Lógica dos comandos (usando lock onde há escrita/leitura do db)
# ------------------------------------------------------------
def handle_update_position(payload):
    required = ["vehicle_id", "lat", "lon", "timestamp", "status"]
    ok, msg = validate_fields(payload, required)
    if not ok:
        return {"error": msg}

    ok2, msg2 = validate_lat_lon(payload["lat"], payload["lon"])
    if not ok2:
        return {"error": msg2}

    vid = str(payload["vehicle_id"])
    # construir registro padronizado
    record = {
        "vehicle_id": vid,
        "lat": float(payload["lat"]),
        "lon": float(payload["lon"]),
        "timestamp": payload["timestamp"],
        "status": payload["status"]
    }

    with db_lock:
        db["vehicles"][vid] = record
        save_db_atomic(db)

    log(f"Posição atualizada: {vid}")
    return {"ok": True}

def handle_get_vehicle(payload):
    vid = payload.get("vehicle_id")
    if not vid:
        return {"error": "vehicle_id ausente"}

    with db_lock:
        vehicle = db["vehicles"].get(str(vid))

    if not vehicle:
        return {"error": "Veículo não encontrado"}

    return {"vehicle": vehicle}

def handle_create_order(payload):
    required = ["order_id", "client_name", "address"]
    ok, msg = validate_fields(payload, required)
    if not ok:
        return {"error": msg}

    oid = str(payload["order_id"])
    with db_lock:
        if oid in db["orders"]:
            return {"error": "Ordem já existe"}

        order = {
            "order_id": oid,
            "client_name": payload["client_name"],
            "address": payload["address"],
            "status": "pending",
            "created_at": now()
        }
        db["orders"][oid] = order
        save_db_atomic(db)

    log(f"Ordem criada: {oid}")
    return {"ok": True}

def handle_update_order(payload):
    required = ["order_id", "status"]
    ok, msg = validate_fields(payload, required)
    if not ok:
        return {"error": msg}

    oid = str(payload["order_id"])

    with db_lock:
        if oid not in db["orders"]:
            return {"error": "Ordem não encontrada"}
        db["orders"][oid]["status"] = payload["status"]
        db["orders"][oid]["last_update"] = now()
        save_db_atomic(db)

    log(f"Ordem atualizada: {oid} -> {payload['status']}")
    return {"ok": True}

def handle_get_order(payload):
    oid = payload.get("order_id")
    if not oid:
        return {"error": "order_id ausente"}

    with db_lock:
        order = db["orders"].get(str(oid))

    if not order:
        return {"error": "Ordem não encontrada"}

    return {"order": order}

def handle_list_orders(payload):
    with db_lock:
        orders = list(db["orders"].values())
    return {"orders": orders}

def handle_list_vehicles(payload):
    with db_lock:
        vehicles = list(db["vehicles"].values())
    return {"vehicles": vehicles}

# ------------------------------------------------------------
# Processador de mensagens com proteção extra
# ------------------------------------------------------------
def process_request(req):
    # Remova ou comente prints de debug em produção
    # print("=== DEBUG: process_request ===")
    # print("REQ recebido:", req)
    # print("DB atual:", db)
    # print("==============================")

    if not isinstance(req, dict):
        return {"error": "Requisição deve ser um objeto JSON"}

    if "type" not in req or "payload" not in req:
        return {"error": "Formato inválido. Esperado: {type, payload}"}

    t = req["type"]

    try:
        if t == "update_position":
            return handle_update_position(req["payload"])
        if t == "get_vehicle":
            return handle_get_vehicle(req["payload"])
        if t == "create_order":
            return handle_create_order(req["payload"])
        if t == "update_order":
            return handle_update_order(req["payload"])
        if t == "get_order":
            return handle_get_order(req["payload"])
        if t == "list_orders":
            return handle_list_orders(req["payload"])
        if t == "list_vehicles":
            return handle_list_vehicles(req["payload"])

        return {"error": f"Comando desconhecido: {t}"}
    except Exception as e:
        # garantir que exceção não derrube o cliente
        log(f"ERRO interno em process_request: {e}")
        return {"error": f"Erro interno: {e}"}

# ------------------------------------------------------------
# Worker por conexão (manuseio de buffer e proteção contra mensagens grandes)
# ------------------------------------------------------------
def handle_client(conn, addr):
    log(f"Cliente conectado: {addr}")
    buffer = ""

    # opcional: definir timeout para recv (evita bloqueio indefinido)
    conn.settimeout(300)  # 5 minutos

    try:
        while True:
            try:
                data = conn.recv(4096)
            except socket.timeout:
                log(f"Timeout de conexão com {addr}, encerrando.")
                break
            except Exception as e:
                log(f"Erro recv {addr}: {e}")
                break

            if not data:
                break

            try:
                chunk = data.decode("utf-8")
            except Exception:
                resp = {"error": "Payload não é UTF-8"}
                conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))
                continue

            buffer += chunk

            # proteção: se buffer crescer demais, rejeitar
            if len(buffer) > MAX_MSG_SIZE:
                resp = {"error": "Mensagem muito grande"}
                conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))
                log(f"Mensagem muito grande de {addr}; desconectando.")
                break

            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if not line.strip():
                    continue

                try:
                    req = json.loads(line)
                except json.JSONDecodeError as jde:
                    resp = {"error": f"JSON inválido: {jde.msg}"}
                    conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))
                    continue
                except Exception as e:
                    resp = {"error": f"Falha ao decodificar JSON: {e}"}
                    conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))
                    continue

                resp = process_request(req)
                try:
                    conn.sendall((json.dumps(resp, ensure_ascii=False) + "\n").encode("utf-8"))
                except Exception as e:
                    log(f"Erro enviando resposta para {addr}: {e}")
                    break

    except Exception as e:
        log(f"Erro com cliente {addr}: {e}")

    finally:
        try:
            conn.close()
        except:
            pass
        log(f"Cliente desconectado: {addr}")

# ------------------------------------------------------------
# Servidor principal (com REUSEADDR e shutdown gracioso)
# ------------------------------------------------------------
def start_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # permite reusar a porta rapidamente
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen()
    log(f"Servidor TCP iniciado em {HOST}:{PORT}")

    try:
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        log("Servidor interrompido pelo usuário.")
    except Exception as e:
        log(f"Erro no servidor principal: {e}")
    finally:
        try:
            s.close()
        except:
            pass
        log("Servidor finalizado.")

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
if __name__ == "__main__":
    start_server()
