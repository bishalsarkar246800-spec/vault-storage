import asyncio, hashlib, multiprocessing, os, shutil, time
from typing import Dict, List, Set
from fastapi import FastAPI, File, HTTPException, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import httpx, uvicorn

REPLICATION_FACTOR = 3
WRITE_QUORUM = 2
READ_QUORUM = 1

STORAGE_NODES = [
    {"id": "node-1", "host": "127.0.0.1", "port": 9001, "dir": "./vault_data_1"},
    {"id": "node-2", "host": "127.0.0.1", "port": 9002, "dir": "./vault_data_2"},
    {"id": "node-3", "host": "127.0.0.1", "port": 9003, "dir": "./vault_data_3"},
]
COORDINATOR_PORT = 8000

METADATA_STORE: Dict[str, dict] = {}
HEALTHY_NODES: Set[str] = set()
SIMULATED_PARTITIONS: Set[str] = set()
WRITE_LOCKS: Dict[str, asyncio.Lock] = {}

def get_node_app(node_id: str, data_dir: str):
    node = FastAPI(title=f"Vault OSD - {node_id}")
    os.makedirs(data_dir, exist_ok=True)

    @node.get("/ping")
    def ping():
        return {"status": "ok", "node_id": node_id}

    @node.get("/chunks/{chunk_id}/verify")
    def verify_chunk(chunk_id: str):
        file_path = os.path.join(data_dir, f"{chunk_id}.dat")
        meta_path = os.path.join(data_dir, f"{chunk_id}.sha256")
        if not os.path.exists(file_path) or not os.path.exists(meta_path):
            raise HTTPException(status_code=404, detail="Missing chunk")
        with open(file_path, "rb") as f:
            data = f.read()
        with open(meta_path, "r") as f:
            checksum = f.read().strip()
        if hashlib.sha256(data).hexdigest() != checksum:
            raise HTTPException(status_code=500, detail="Corrupted")
        return {"status": "valid"}

    @node.put("/chunks/{chunk_id}")
    async def write_chunk(chunk_id: str, file: UploadFile = File(...)):
        payload = await file.read()
        checksum = hashlib.sha256(payload).hexdigest()
        file_path = os.path.join(data_dir, f"{chunk_id}.dat")
        meta_path = os.path.join(data_dir, f"{chunk_id}.sha256")
        with open(file_path, "wb") as f:
            f.write(payload)
        with open(meta_path, "w") as f:
            f.write(checksum)
        return {"status": "committed", "checksum": checksum}

    @node.get("/chunks/{chunk_id}")
    def read_chunk(chunk_id: str):
        file_path = os.path.join(data_dir, f"{chunk_id}.dat")
        meta_path = os.path.join(data_dir, f"{chunk_id}.sha256")
        if not os.path.exists(file_path) or not os.path.exists(meta_path):
            raise HTTPException(status_code=404, detail="Not found")
        with open(file_path, "rb") as f:
            data = f.read()
        with open(meta_path, "r") as f:
            checksum = f.read().strip()
        if hashlib.sha256(data).hexdigest() != checksum:
            raise HTTPException(status_code=500, detail="Bit rot detected")
        return Response(content=data, media_type="application/octet-stream")

    return node

def get_coordinator_app():
    coordinator = FastAPI(title="Vault Coordinator Engine")

    coordinator.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    async def heartbeat_loop():
        async with httpx.AsyncClient() as client:
            while True:
                for node in STORAGE_NODES:
                    if node["id"] in SIMULATED_PARTITIONS:
                        HEALTHY_NODES.discard(node["id"])
                        continue
                    try:
                        res = await client.get(f"http://{node['host']}:{node['port']}/ping", timeout=0.5)
                        if res.status_code == 200:
                            HEALTHY_NODES.add(node["id"])
                        else:
                            HEALTHY_NODES.discard(node["id"])
                    except Exception:
                        HEALTHY_NODES.discard(node["id"])
                await asyncio.sleep(1.0)

    async def anti_entropy_repair_loop():
        async with httpx.AsyncClient() as client:
            while True:
                await asyncio.sleep(2.0)
                for object_id, meta in list(METADATA_STORE.items()):
                    valid_nodes = []
                    for node in STORAGE_NODES:
                        nid = node["id"]
                        if nid not in HEALTHY_NODES or nid in SIMULATED_PARTITIONS:
                            continue
                        try:
                            res = await client.get(
                                f"http://{node['host']}:{node['port']}/chunks/{object_id}/verify", timeout=0.5
                            )
                            if res.status_code == 200:
                                valid_nodes.append(nid)
                        except Exception:
                            pass

                    meta["replicas"] = valid_nodes

                    if 0 < len(valid_nodes) < REPLICATION_FACTOR:
                        donor = next(n for n in STORAGE_NODES if n["id"] == valid_nodes[0])
                        missing = [
                            n for n in STORAGE_NODES
                            if n["id"] in HEALTHY_NODES and n["id"] not in SIMULATED_PARTITIONS and n["id"] not in valid_nodes
                        ]
                        for target in missing:
                            try:
                                chunk_res = await client.get(
                                    f"http://{donor['host']}:{donor['port']}/chunks/{object_id}", timeout=2.0
                                )
                                if chunk_res.status_code == 200:
                                    write_res = await client.put(
                                        f"http://{target['host']}:{target['port']}/chunks/{object_id}",
                                        files={"file": (object_id, chunk_res.content, "application/octet-stream")},
                                        timeout=3.0
                                    )
                                    if write_res.status_code == 200:
                                        if target["id"] not in meta["replicas"]:
                                            meta["replicas"].append(target["id"])
                                        print(f"[AUTO-HEAL] Reconstructed {object_id} -> {target['id']}")
                            except Exception:
                                pass

    @coordinator.on_event("startup")
    async def startup_daemons():
        asyncio.create_task(heartbeat_loop())
        asyncio.create_task(anti_entropy_repair_loop())

    @coordinator.get("/", response_class=FileResponse)
    def serve_dashboard():
        return FileResponse("index.html")

    @coordinator.get("/health")
    def health_check():
        return {
            "status": "HEALTHY" if len(HEALTHY_NODES) >= WRITE_QUORUM else "DEGRADED",
            "active_nodes": list(HEALTHY_NODES),
            "total_objects": len(METADATA_STORE),
        }

    @coordinator.get("/vault/objects")
    def list_objects():
        return {
            obj_id: {
                "checksum": meta["checksum"],
                "replicas": meta["replicas"],
            }
            for obj_id, meta in METADATA_STORE.items()
        }

    @coordinator.post("/vault/upload/{object_id}")
    async def upload(object_id: str, file: UploadFile = File(...)):
        if object_id not in WRITE_LOCKS:
            WRITE_LOCKS[object_id] = asyncio.Lock()

        async with WRITE_LOCKS[object_id]:
            content = await file.read()
            checksum = hashlib.sha256(content).hexdigest()
            active = [n for n in STORAGE_NODES if n["id"] in HEALTHY_NODES and n["id"] not in SIMULATED_PARTITIONS]

            if len(active) < WRITE_QUORUM:
                raise HTTPException(status_code=503, detail="Write quorum unreachable")

            successful = []
            async with httpx.AsyncClient() as client:
                for node in active:
                    try:
                        res = await client.put(
                            f"http://{node['host']}:{node['port']}/chunks/{object_id}",
                            files={"file": (object_id, content, "application/octet-stream")},
                            timeout=3.0,
                        )
                        if res.status_code == 200:
                            successful.append(node["id"])
                    except Exception:
                        continue

            if len(successful) < WRITE_QUORUM:
                raise HTTPException(status_code=500, detail="Write quorum failed")

            METADATA_STORE[object_id] = {
                "checksum": checksum,
                "replicas": successful,
            }
            return {"status": "COMMITTED", "replicas": successful, "checksum": checksum}

    @coordinator.get("/vault/download/{object_id}")
    async def download(object_id: str):
        if object_id not in METADATA_STORE:
            raise HTTPException(status_code=404, detail="Not found")

        meta = METADATA_STORE[object_id]
        eligible = [
            n for n in STORAGE_NODES
            if n["id"] in meta["replicas"] and n["id"] in HEALTHY_NODES and n["id"] not in SIMULATED_PARTITIONS
        ]

        if not eligible:
            raise HTTPException(status_code=503, detail="All replicas unreachable")

        async with httpx.AsyncClient() as client:
            for node in eligible:
                try:
                    res = await client.get(f"http://{node['host']}:{node['port']}/chunks/{object_id}", timeout=2.0)
                    if res.status_code == 200 and hashlib.sha256(res.content).hexdigest() == meta["checksum"]:
                        return Response(content=res.content, media_type="application/octet-stream")
                except Exception:
                    continue

        raise HTTPException(status_code=500, detail="Data corrupted across all available replicas")

    @coordinator.post("/test/partition/{node_id}")
    def partition(node_id: str):
        SIMULATED_PARTITIONS.add(node_id)
        HEALTHY_NODES.discard(node_id)
        return {"status": "partitioned", "node_id": node_id}

    @coordinator.post("/test/heal-network/{node_id}")
    def heal_network(node_id: str):
        SIMULATED_PARTITIONS.discard(node_id)
        HEALTHY_NODES.add(node_id)
        return {"status": "network_restored", "node_id": node_id}

    return coordinator

def run_node_worker(node_id: str, port: int, folder: str):
    app = get_node_app(node_id, folder)
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")

if __name__ == "__main__":
    for node in STORAGE_NODES:
        os.makedirs(node["dir"], exist_ok=True)

    procs = []
    for node in STORAGE_NODES:
        p = multiprocessing.Process(
            target=run_node_worker,
            args=(node["id"], node["port"], node["dir"]),
            daemon=True
        )
        p.start()
        procs.append(p)

    print(">> Initialized Storage OSDs on ports 9001, 9002, 9003")
    print(f">> Serving Vault Dashboard & API Gateway on http://127.0.0.1:{COORDINATOR_PORT}")

    try:
        app = get_coordinator_app()
        uvicorn.run(app, host="127.0.0.1", port=COORDINATOR_PORT, log_level="info")
    finally:
        for p in procs:
            p.terminate()