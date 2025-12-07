import socket
import json

HOST = "127.0.0.1"
PORT = 5000

def send_request(sock, req_dict):
    """Envia um JSON + '\n' e aguarda resposta."""
    msg = json.dumps(req_dict) + "\n"
    sock.sendall(msg.encode("utf-8"))

    buffer = ""
    while True:
        data = sock.recv(4096).decode("utf-8")
        if not data:
            raise Exception("Servidor desconectou")
        buffer += data
        if "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            return json.loads(line)

def menu():
    print("\n======= MENU TCP =======")
    print("1 - Atualizar posição do veículo")
    print("2 - Buscar veículo")
    print("3 - Criar ordem")
    print("4 - Atualizar ordem")
    print("5 - Buscar ordem")
    print("6 - Listar ordens")
    print("7 - Listar veículos")
    print("0 - Sair")
    print("========================")

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    print("[CLIENTE] Conectado ao servidor.")

    try:
        while True:
            menu()
            opc = input("Escolha uma opção: ")

            # ---------------------------
            # SAIR
            # ---------------------------
            if opc == "0":
                print("[CLIENTE] Encerrando conexão.")
                break

            # ---------------------------
            # 1 - Atualizar posição
            # ---------------------------
            elif opc == "1":
                req = {
                    "type": "update_position",
                    "payload": {
                        "vehicle_id": input("ID do veículo: "),
                        "lat": float(input("Latitude: ")),
                        "lon": float(input("Longitude: ")),
                        "timestamp": input("Timestamp: "),
                        "status": input("Status: ")
                    }
                }

            # ---------------------------
            # 2 - Buscar veículo
            # ---------------------------
            elif opc == "2":
                req = {
                    "type": "get_vehicle",
                    "payload": {
                        "vehicle_id": input("ID do veículo: ")
                    }
                }

            # ---------------------------
            # 3 - Criar ordem
            # ---------------------------
            elif opc == "3":
                req = {
                    "type": "create_order",
                    "payload": {
                        "order_id": input("ID da ordem: "),
                        "client_name": input("Cliente: "),
                        "address": input("Endereço: ")
                    }
                }

            # ---------------------------
            # 4 - Atualizar ordem
            # ---------------------------
            elif opc == "4":
                req = {
                    "type": "update_order",
                    "payload": {
                        "order_id": input("ID da ordem: "),
                        "status": input("Status atualizado: ")
                    }
                }

            # ---------------------------
            # 5 - Buscar ordem
            # ---------------------------
            elif opc == "5":
                req = {
                    "type": "get_order",
                    "payload": {
                        "order_id": input("ID da ordem: ")
                    }
                }

            # ---------------------------
            # 6 - Listar ordens
            # ---------------------------
            elif opc == "6":
                req = {"type": "list_orders", "payload": {}}

            # ---------------------------
            # 7 - Listar veículos
            # ---------------------------
            elif opc == "7":
                req = {"type": "list_vehicles", "payload": {}}

            else:
                print("Opção inválida!")
                continue

            # Envia requisição e recebe resposta
            resp = send_request(sock, req)
            print("\n[RESPOSTA]:")
            print(json.dumps(resp, indent=4))

    except Exception as e:
        print("[ERRO]", str(e))

    finally:
        sock.close()
        print("[CLIENTE] Desconectado.")


if __name__ == "__main__":
    main()
