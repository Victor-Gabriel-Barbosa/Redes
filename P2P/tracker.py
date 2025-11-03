# tracker.py
# Um tracker TCP simples: aceita comandos de texto:
# ANNOUNCE <file_id> <host> <port>
# GETPEERS <file_id>
# Respostas: "OK" ou lista de peers "host:port" separados por linhas.

import asyncio

PEERS = {}  # file_id -> set of "host:port"

async def handle(reader, writer):
    data = await reader.readline()
    if not data:
        writer.close()
        await writer.wait_closed()
        return
    line = data.decode().strip()
    parts = line.split()
    if not parts:
        writer.close()
        await writer.wait_closed()
        return

    cmd = parts[0].upper()
    if cmd == "ANNOUNCE" and len(parts) == 4:
        _, file_id, host, port = parts
        PEERS.setdefault(file_id, set()).add(f"{host}:{port}")
        writer.write(b"OK\n")
        await writer.drain()
    elif cmd == "GETPEERS" and len(parts) == 2:
        _, file_id = parts
        peers = PEERS.get(file_id, set())
        for p in peers:
            writer.write(f"{p}\n".encode())
        await writer.drain()
    else:
        writer.write(b"ERR\n")
        await writer.drain()
    writer.close()
    await writer.wait_closed()

async def main(host="0.0.0.0", port=9000):
    server = await asyncio.start_server(handle, host, port)
    addr = server.sockets[0].getsockname()
    print(f"Tracker rodando em {addr}")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Tracker encerrado.")
