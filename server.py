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

            # ── File / Image transfer ──
            terminator = b"\nEND_OF_TRANSFER\n"
            while terminator in buffer:
                before, buffer = buffer.split(terminator, 1)
                newline_pos = before.find(b"\n")
                if newline_pos != -1:
                    header    = before[:newline_pos].decode(errors="ignore").strip()
                    file_data = before[newline_pos + 1:]
                else:
                    header    = before.decode(errors="ignore").strip()
                    file_data = b""

                # Broadcast header + data + terminator as-is to others
                full = (header + "\n").encode() + file_data + terminator
                broadcast(full, sender=conn)
                print(f"[File] {header} from {addr} → {len(file_data)} bytes", flush=True)

            # ── Text messages (line by line) ──
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                line_str = line.decode(errors="ignore").strip()

                if not line_str:
                    continue

                # Ignore stray TYPE: headers (shouldn't reach here normally)
                if line_str.startswith("TYPE:"):
                    continue

                # Broadcast the message exactly as sent by client (already has username)
                # Client sends: "Ahmed: hello\n"
                broadcast((line_str + "\n").encode(), sender=conn)
                print(f"[MSG] {line_str}", flush=True)

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
            except Exception:
                dead.append(c)
        for c in dead:
            clients.remove(c)


def main():
    port = int(os.environ.get("RAILWAY_TCP_APPLICATION_PORT", 3000))
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
