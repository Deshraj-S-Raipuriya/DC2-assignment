# client.py
# Usage: python client.py <my_id> <dme_port> <server_host> <server_port> <peer1_host> <peer1_port> <peer1_id> [<peer2_host> <peer2_port> <peer2_id> ...]
#
# Example (two peers):
# python client.py Joel 9101 127.0.0.1 9000 127.0.0.1 9102 Alice 127.0.0.1 9103 Bob

import sys
import socket
import json
import threading
from datetime import datetime
from dme import LamportDME

def send_server_view(server_host, server_port):
    req = {"action":"VIEW"}
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((server_host, server_port))
    s.sendall(json.dumps(req).encode())
    data = s.recv(65536).decode()
    s.close()
    resp = json.loads(data)
    if resp.get("status") == "OK":
        return resp.get("content")
    else:
        raise Exception(resp.get("message","unknown"))

def send_server_post(server_host, server_port, user_id, text, client_ts):
    req = {"action":"POST", "user_id": user_id, "text": text, "client_ts": client_ts}
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((server_host, server_port))
    s.sendall(json.dumps(req).encode())
    data = s.recv(65536).decode()
    s.close()
    return json.loads(data)

def cli_loop(my_id, dme, server_host, server_port):
    print(f"[{my_id}] Enter commands: view  OR  post <text>   (type 'exit' to quit)")
    while True:
        try:
            line = input(f"{my_id}> ").strip()
        except EOFError:
            break
        if not line:
            continue
        if line.lower() == "exit":
            break
        if line.lower() == "view":
            content = send_server_view(server_host, server_port)
            print("=== Shared Chat File ===")
            print(content if content.strip() else "(empty)")
            print("========================")
        elif line.lower().startswith("post "):
            txt = line[5:].strip()
            # Acquire DME
            print("[DME] Requesting critical section...")
            ts = dme.request_critical_section()
            # create client timestamp for the post
            client_ts = datetime.now().strftime("%d %b %I:%M:%S%p")
            print(f"[DME] Entered critical section (ts={ts}). Posting to server...")
            resp = send_server_post(server_host, server_port, my_id, txt, client_ts)
            print("[SERVER] Response:", resp)
            # release
            dme.release_critical_section(ts)
            print("[DME] Released critical section.")
        else:
            print("Unknown command. Use 'view' or 'post <text>' or 'exit'")

if __name__ == "__main__":
    if len(sys.argv) < 8 or (len(sys.argv)-5) % 3 != 0:
        print("Usage: python client.py <my_id> <dme_port> <server_host> <server_port> <peer1_host> <peer1_port> <peer1_id> [<peer2_host> <peer2_port> <peer2_id> ...]")
        sys.exit(1)
    my_id = sys.argv[1]
    dme_port = int(sys.argv[2])
    server_host = sys.argv[3]
    server_port = int(sys.argv[4])
    peers = []
    i = 5
    while i < len(sys.argv):
        host = sys.argv[i]; port = int(sys.argv[i+1]); pid = sys.argv[i+2]
        peers.append({"host": host, "port": port, "id": pid})
        i += 3

    dme = LamportDME(my_id, dme_port, peers)
    try:
        cli_loop(my_id, dme, server_host, server_port)
    except KeyboardInterrupt:
        pass
    finally:
        dme.stop()
        print("Exiting client.")
