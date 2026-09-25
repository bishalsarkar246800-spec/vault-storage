import asyncio, hashlib, multiprocessing, os, shutil, time
from typing import Dict, List, Set
from fastapi import FastAPI, File, HTTPException, Response, UploadFile
import httpx, pytest, uvicorn

REPLICATION_FACTOR = 3
WRITE_QUORUM = 2
READ_QUORUM = 1

STORAGE_NODES = [
    {"id": "node-1", "host": "127.0.0.1", "port": 9201, "dir": "./data_test_node_1"},
    {"id": "node-2", "host": "127.0.0.1", "port": 9202, "dir": "./data_test_node_2"},
    {"id": "node-3", "host": "127.0.0.1", "port": 9203, "dir": "./data_test_node_3"},
]
COORDINATOR_PORT = 8200
COORDINATOR_URL = f"http://127.0.0.1:{COORDINATOR_PORT}"

METADATA_STORE: Dict[str, dict] = {}
HEALTHY_NODES: Set[str] = set()
SIMULATED_PARTITIONS: Set[str] = set()
WRITE_LOCKS: Dict[str, asyncio.Lock] = {}

def get_node_app(node_id: str, data_dir: str):
    node = FastAPI()
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
    coordinator = FastAPI()

    async def heartbeat_loop():
        async with httpx.AsyncClient() as client:
            while True:
                for node in STORAGE_NODES:
                    if node["id"] in SIMULATED_PARTITIONS:
                        HEALTHY_NODES.discard(node["id"])
                        continue
                    try:
                        res = await client.get(f"http://{node['host']}:{node['port']}/ping", timeout=0.1)
                        if res.status_code == 200:
                            HEALTHY_NODES.add(node["id"])
                        else:
                            HEALTHY_NODES.discard(node["id"])
                    except Exception:
                        HEALTHY_NODES.discard(node["id"])
                await asyncio.sleep(0.05)

    async def anti_entropy_repair_loop():
        async with httpx.AsyncClient() as client:
            while True:
                await asyncio.sleep(0.05)
                for object_id, meta in list(METADATA_STORE.items()):
                    valid_nodes = []
                    for node in STORAGE_NODES:
                        nid = node["id"]
                        if nid not in HEALTHY_NODES or nid in SIMULATED_PARTITIONS:
                            continue
                        try:
                            res = await client.get(
                                f"http://{node['host']}:{node['port']}/chunks/{object_id}/verify", timeout=0.1
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
                            if n["id"] in HEALTHY_NODES
                            and n["id"] not in SIMULATED_PARTITIONS
                            and n["id"] not in valid_nodes
                        ]
                        for target in missing:
                            try:
                                chunk_res = await client.get(
                                    f"http://{donor['host']}:{donor['port']}/chunks/{object_id}", timeout=0.5
                                )
                                if chunk_res.status_code == 200:
                                    write_res = await client.put(
                                        f"http://{target['host']}:{target['port']}/chunks/{object_id}",
                                        files={"file": (object_id, chunk_res.content, "application/octet-stream")},
                                        timeout=1.0
                                    )
                                    if write_res.status_code == 200:
                                        if target["id"] not in meta["replicas"]:
                                            meta["replicas"].append(target["id"])
                            except Exception:
                                pass

    @coordinator.on_event("startup")
    async def startup_daemons():
        asyncio.create_task(heartbeat_loop())
        asyncio.create_task(anti_entropy_repair_loop())

    @coordinator.get("/ping")
    def ping():
        return {"status": "ok", "ready": len(HEALTHY_NODES) >= WRITE_QUORUM}

    @coordinator.post("/upload/{object_id}")
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
                            timeout=1.0,
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
            return {"status": "COMMITTED", "replicas": successful}

    @coordinator.get("/download/{object_id}")
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
                    res = await client.get(f"http://{node['host']}:{node['port']}/chunks/{object_id}", timeout=0.5)
                    if res.status_code == 200 and hashlib.sha256(res.content).hexdigest() == meta["checksum"]:
                        return Response(content=res.content, media_type="application/octet-stream")
                except Exception:
                    continue

        raise HTTPException(status_code=500, detail="Data corrupted across all available replicas")

    @coordinator.post("/test/partition/{node_id}")
    def partition(node_id: str):
        SIMULATED_PARTITIONS.add(node_id)
        HEALTHY_NODES.discard(node_id)
        return {"status": "partitioned"}

    @coordinator.post("/test/heal-network/{node_id}")
    def heal_network(node_id: str):
        SIMULATED_PARTITIONS.discard(node_id)
        HEALTHY_NODES.add(node_id)
        return {"status": "network_restored"}

    return coordinator

def run_node_worker(node_id: str, port: int, folder: str):
    app = get_node_app(node_id, folder)
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="error")

def run_coordinator_worker(port: int):
    app = get_coordinator_app()
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="error")

@pytest.fixture(scope="session", autouse=True)
def setup_cluster():
    for node in STORAGE_NODES:
        shutil.rmtree(node["dir"], ignore_errors=True)

    processes = []
    for node in STORAGE_NODES:
        p = multiprocessing.Process(
            target=run_node_worker,
            args=(node["id"], node["port"], node["dir"]),
        )
        p.daemon = True
        p.start()
        processes.append(p)

    coord_p = multiprocessing.Process(
        target=run_coordinator_worker,
        args=(COORDINATOR_PORT,),
    )
    coord_p.daemon = True
    coord_p.start()
    processes.append(coord_p)

    with httpx.Client() as client:
        for _ in range(50):
            try:
                res = client.get(f"{COORDINATOR_URL}/ping", timeout=0.1)
                if res.status_code == 200 and res.json().get("ready"):
                    break
            except Exception:
                time.sleep(0.02)

    yield

    for p in processes:
        p.terminate()

@pytest.mark.asyncio
async def test_case_1_normal_quorum_write_and_read():
    payload = b"Payload for baseline quorum validation"
    async with httpx.AsyncClient() as client:
        res = await client.post(
            f"{COORDINATOR_URL}/upload/item_01",
            files={"file": ("item_01", payload, "application/octet-stream")},
        )
        assert res.status_code == 200
        assert len(res.json()["replicas"]) >= WRITE_QUORUM

        get_res = await client.get(f"{COORDINATOR_URL}/download/item_01")
        assert get_res.status_code == 200
        assert get_res.content == payload

@pytest.mark.asyncio
async def test_case_2_silent_bitrot_corruption_failover():
    payload = b"Critical cryptographic key payload"
    async with httpx.AsyncClient() as client:
        await client.post(
            f"{COORDINATOR_URL}/upload/key_vault",
            files={"file": ("key_vault", payload, "application/octet-stream")},
        )

        corrupt_target = os.path.join(STORAGE_NODES[0]["dir"], "key_vault.dat")
        with open(corrupt_target, "wb") as f:
            f.write(b"CORRUPTED_BYTES_INSERTED_BY_NEMESIS")

        read_res = await client.get(f"{COORDINATOR_URL}/download/key_vault")
        assert read_res.status_code == 200
        assert read_res.content == payload

@pytest.mark.asyncio
async def test_case_3_network_partition_minority_survives():
    async with httpx.AsyncClient() as client:
        await client.post(f"{COORDINATOR_URL}/test/partition/node-3")

        payload = b"Surviving partition write"
        res = await client.post(
            f"{COORDINATOR_URL}/upload/partition_doc",
            files={"file": ("partition_doc", payload, "application/octet-stream")},
        )
        assert res.status_code == 200
        assert "node-3" not in res.json()["replicas"]

        await client.post(f"{COORDINATOR_URL}/test/heal-network/node-3")

@pytest.mark.asyncio
async def test_case_4_quorum_exhaustion_prevents_dirty_writes():
    async with httpx.AsyncClient() as client:
        await client.post(f"{COORDINATOR_URL}/test/partition/node-2")
        await client.post(f"{COORDINATOR_URL}/test/partition/node-3")

        res = await client.post(
            f"{COORDINATOR_URL}/upload/should_fail",
            files={"file": ("should_fail", b"lost write", "application/octet-stream")},
        )
        assert res.status_code == 503

        await client.post(f"{COORDINATOR_URL}/test/heal-network/node-2")
        await client.post(f"{COORDINATOR_URL}/test/heal-network/node-3")

@pytest.mark.asyncio
async def test_case_5_self_healing_anti_entropy_reconstruction():
    payload = b"Data that must self-heal automatically"
    async with httpx.AsyncClient() as client:
        res = await client.post(
            f"{COORDINATOR_URL}/upload/heal_target",
            files={"file": ("heal_target", payload, "application/octet-stream")},
        )
        assert res.status_code == 200
        
        node2_file = os.path.join(STORAGE_NODES[1]["dir"], "heal_target.dat")
        assert os.path.exists(node2_file)

        shutil.rmtree(STORAGE_NODES[1]["dir"])
        os.makedirs(STORAGE_NODES[1]["dir"], exist_ok=True)
        assert not os.path.exists(node2_file)

        reconstructed = False
        for _ in range(50):
            await asyncio.sleep(0.02)
            if os.path.exists(node2_file):
                with open(node2_file, "rb") as f:
                    if f.read() == payload:
                        reconstructed = True
                        break

        assert reconstructed