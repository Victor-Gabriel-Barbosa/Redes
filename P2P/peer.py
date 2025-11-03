# peer.py
# Uso:
# 1) Seeder (tem arquivo): python peer.py --file arquivo.bin --tracker 127.0.0.1:9000 --port 10001
# 2) Leecher (quer baixar): python peer.py --file nome_destino.bin --tracker 127.0.0.1:9000 --port 10002 --fileid <id>
#
# O peer fará:
# - Servir chunks via TCP (protocolo simples).
# - Anunciar ao tracker.
# - Se leecher: pedir peers ao tracker e requisitar chunks em paralelo.

import argparse
import asyncio
import hashlib
import math
import os
from pathlib import Path

CHUNK_SIZE = 1024 * 1024  # 1 MiB

# Simple protocol helpers: send length-prefixed byte messages
async def read_exact(reader, n):
    data = b""
    while len(data) < n:
        part = await reader.read(n - len(data))
        if not part:
            raise ConnectionError("Conexão fechada prematuramente")
        data += part
    return data

async def send_message(writer, b: bytes):
    writer.write(len(b).to_bytes(8, "big") + b)
    await writer.drain()

async def read_message(reader):
    lenb = await read_exact(reader, 8)
    length = int.from_bytes(lenb, "big")
    return await read_exact(reader, length)

def file_metadata(path: Path):
    size = path.stat().st_size
    chunks = math.ceil(size / CHUNK_SIZE)
    hashes = []
    with path.open("rb") as f:
        for _ in range(chunks):
            chunk = f.read(CHUNK_SIZE)
            hashes.append(hashlib.sha1(chunk).hexdigest())
    return {"size": size, "chunks": chunks, "hashes": hashes}

# Serve chunks: aceita mensagens simples:
# "GETCHUNK <file_id> <index>\n" -> responderá com mensagem length-prefixed contendo o chunk bytes.
async def handle_peer_server(reader, writer, seeder_files):
    addr = writer.get_extra_info("peername")
    try:
        line = (await reader.readline()).decode().strip()
        if line.startswith("GETCHUNK"):
            _, file_id, idx_s = line.split()
            idx = int(idx_s)
            # encontrar arquivo associado em seeder_files[file_id] -> path
            if file_id not in seeder_files:
                writer.write(b"ERR\n")
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                return
            path = seeder_files[file_id]
            with path.open("rb") as f:
                f.seek(idx * CHUNK_SIZE)
                data = f.read(CHUNK_SIZE)
            # enviar chunk length-prefixed
            await send_message(writer, data)
        elif line.startswith("GETMETA"):
            # retorna metadata json simples (size|chunks|hash1,hash2,...)
            _, file_id = line.split()
            meta = seeder_files.get(file_id + "_meta")
            if not meta:
                writer.write(b"ERR\n")
                await writer.drain()
            else:
                # tamanho|chunks|hash1,hash2,...
                msg = f"{meta['size']}|{meta['chunks']}|{','.join(meta['hashes'])}".encode()
                await send_message(writer, msg)
    except Exception as e:
        # simples log
        print("Erro handler:", e)
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except:
            pass

# Funcs de tracker
async def tracker_announce(tracker_host, tracker_port, file_id, host, port):
    reader, writer = await asyncio.open_connection(tracker_host, tracker_port)
    writer.write(f"ANNOUNCE {file_id} {host} {port}\n".encode())
    await writer.drain()
    resp = await reader.readline()
    writer.close()
    await writer.wait_closed()
    return resp.decode().strip()

async def tracker_get_peers(tracker_host, tracker_port, file_id):
    reader, writer = await asyncio.open_connection(tracker_host, tracker_port)
    writer.write(f"GETPEERS {file_id}\n".encode())
    await writer.drain()
    peers = []
    while True:
        line = await reader.readline()
        if not line:
            break
        peers.append(line.decode().strip())
    writer.close()
    await writer.wait_closed()
    return peers

# Baixar metadata de um peer
async def fetch_meta_from_peer(host, port, file_id):
    try:
        reader, writer = await asyncio.open_connection(host, int(port))
        writer.write(f"GETMETA {file_id}\n".encode())
        await writer.drain()
        data = await read_message(reader)
        writer.close()
        await writer.wait_closed()
        s = data.decode()
        size_s, chunks_s, hashes_s = s.split("|")
        return {"size": int(size_s), "chunks": int(chunks_s), "hashes": hashes_s.split(",")}
    except Exception:
        return None

# Baixar um chunk específico de um peer
async def fetch_chunk_from_peer(host, port, file_id, idx):
    try:
        reader, writer = await asyncio.open_connection(host, int(port))
        writer.write(f"GETCHUNK {file_id} {idx}\n".encode())
        await writer.drain()
        data = await read_message(reader)
        writer.close()
        await writer.wait_closed()
        return data
    except Exception:
        return None

async def run_peer(args):
    # preparar seeder_files: um dict file_id -> path e file_id_meta -> metadata
    seeder_files = {}
    file_id = args.fileid or (args.file and Path(args.file).name)
    if args.file:
        path = Path(args.file)
        meta = file_metadata(path)
        seeder_files[file_id] = path
        seeder_files[file_id + "_meta"] = meta
        print(f"Modo SEEDER: {path} chunks={meta['chunks']}")
    else:
        print("Modo LEECHER (não tem arquivo local).")

    # iniciar servidor para servir chunks/metadata
    server = await asyncio.start_server(lambda r, w: handle_peer_server(r, w, seeder_files), host="0.0.0.0", port=args.port)
    print(f"Peer servindo na porta {args.port}")

    # anunciar ao tracker
    tr_host, tr_port = args.tracker.split(":")
    announce_resp = await tracker_announce(tr_host, int(tr_port), file_id, args.host, args.port)
    print("Anunciado no tracker:", announce_resp)

    # se leecher: descobrir peers, obter metadata e baixar
    if not args.file:
        # descobrir peers
        peers = await tracker_get_peers(tr_host, int(tr_port), file_id)
        peers = [p for p in peers if p and not p.startswith(f"{args.host}:{args.port}")]  # remover self
        print("Peers encontrados:", peers)
        if not peers:
            print("Nenhum peer disponível.")
            return

        # tentar obter metadata (do primeiro que responder)
        meta = None
        for p in peers:
            h, pt = p.split(":")
            meta = await fetch_meta_from_peer(h, pt, file_id)
            if meta:
                print("Metadata obtida de", p)
                break
        if not meta:
            print("Não foi possível obter metadata.")
            return

        chunks = meta["chunks"]
        hashes = meta["hashes"]
        dest = Path(args.file)
        dest.parent.mkdir(parents=True, exist_ok=True)

        # placeholder de chunks (None = não baixado)
        chunk_store = [None] * chunks

        # função que tenta baixar um chunk de qualquer peer disponível
        async def obtain_chunk(idx):
            # obter lista de peers atualizados a cada tentativa
            peers_live = await tracker_get_peers(tr_host, int(tr_port), file_id)
            for p in peers_live:
                if not p:
                    continue
                h, pt = p.split(":")
                # tentar pegar chunk
                data = await fetch_chunk_from_peer(h, pt, file_id, idx)
                if data:
                    # checar hash
                    hsh = hashlib.sha1(data).hexdigest()
                    if hsh == hashes[idx]:
                        print(f"Chunk {idx} baixado de {h}:{pt} (tamanho {len(data)})")
                        return data
                    else:
                        print(f"Hash mismatch chunk {idx} de {h}:{pt}")
            return None

        # estratégia: baixar vários chunks em paralelo, limitando concorrência
        sem = asyncio.Semaphore(10)  # controla quantos downloads simultâneos
        async def worker(idx):
            async with sem:
                data = await obtain_chunk(idx)
                if data is None:
                    raise RuntimeError(f"Falha ao baixar chunk {idx}")
                chunk_store[idx] = data

        # criar tarefas para todos os chunks
        tasks = [asyncio.create_task(worker(i)) for i in range(chunks)]
        # aguardar concluírem (pode falhar)
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            print("Erro ao baixar:", e)
            return

        # gravar arquivo
        with dest.open("wb") as f:
            for c in chunk_store:
                f.write(c)
        print("Arquivo completado:", dest)

    # manter servidor rodando para servir outros peers
    async with server:
        await server.serve_forever()

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--tracker", required=True, help="tracker_host:tracker_port")
    p.add_argument("--port", type=int, required=True, help="porta para servir chunks")
    p.add_argument("--host", default="127.0.0.1", help="host a anunciar (usado pelo tracker)")
    p.add_argument("--file", help="seeder: caminho do arquivo local / leecher: nome do arquivo destino")
    p.add_argument("--fileid", help="identificador do arquivo (opcional). Padrão: nome do arquivo.")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    try:
        asyncio.run(run_peer(args))
    except KeyboardInterrupt:
        print("Peer encerrado.")
