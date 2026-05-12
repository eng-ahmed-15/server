import socket
import threading
import os

clients = []
clients_lock = threading.Lock()


def handle_client(conn, addr):
    print(f"[+] Connected: {addr}", flush=True)
    with clients_lock:
        clients.append(conn)

    buffer = b""

    while True:
        try:
            chunk = conn.recv(4096)
            if not chunk:
                break
            buffer += chunk

            # ── Text message (ends with \n, no TYPE: header) ──
            while b"\n" in buffer:
                line, rest = buffer.split(b"\n", 1)
                line_str = line.decode(errors="ignore").strip()

                if line_str.startswith("TYPE:"):
                    # ── File or Image transfer ──
                    # Header format: TYPE:IMG;NAME:photo.jpg;HD:1
                    #                TYPE:FILE;NAME:doc.pdf
                    header = line_str
                    # Collect body until END_OF_TRANSFER
                    body_buf = rest
                    terminator = b"\nEND_OF_TRANSFER\n"

                    while terminator not in body_buf:
                        more = conn.recv(65536)
                        if not more:
                            break
                        body_buf += more

                    if terminator in body_buf:
                        file_data, after = body_buf.split(terminator, 1)
                        buffer = after
                        # Broadcast header + data + terminator to others
                        full = (header + "\n").encode() + file_data + terminator
                        broadcast(full, sender=conn)
                        print(f"[File] {header} from {addr} → {len(file_data)} bytes", flush=True)
                    else:
                        buffer = b""
                    break

                else:
                    # Plain text message
                    msg = f"[{addr[0]}:{addr[1]}]: {line_str}\n"
                    print(msg, end="", flush=True)
                    broadcast(msg.encode(), sender=conn)
                    buffer = rest

        except Exception as e:
            print(f"[!] Error from {addr}: {e}", flush=True)
            break

    print(f"[-] Disconnected: {addr}", flush=True)
    with clients_lock:
        if conn in clients:
            clients.remove(conn)
    conn.close()


def broadcast(data: bytes, sender=None):
    with clients_lock:
        dead = []
        for c in clients:
            if c == sender:
                continue
            try:
                c.sendall(data)
            except:
                dead.append(c)
        for c in dead:
            clients.remove(c)


def main():
    port = int(os.environ.get("PORT", 50000))
    host = "0.0.0.0"

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen()
    print(f"[*] Server on {host}:{port}", flush=True)

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    main()
