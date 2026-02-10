# solo-bloomberg

A self-hosted, containerized analytics platform built with a strict, reproducible runtime.
The system uses Docker as the **only** Python environment—no virtual environments, no host installs.

---

## Runtime (Docker) — Locked

The runtime defines **how all code executes**.  
It is immutable, reproducible, and shared by all tools (Prefect, SQLMesh, etc.).

### Principles

- Docker image = Python environment
- `Dockerfile` = execution contract
- `requirements.txt` = dependency truth
- No `python -m venv`
- No `pip install` on host
- No installing packages in running containers
- Rebuild image to change runtime state

---

## Runtime Structure


```text
runtime/
├─ Dockerfile
└─ requirements.txt


docker build -t solo-bloomberg-runtime -f runtime/Dockerfile runtime
docker run --rm solo-bloomberg-runtime prefect version
docker run --rm solo-bloomberg-runtime sqlmesh --version
```

## Runtime Structure

### Step 1 — Initialize the SQLMesh project (create files)

```bash
docker run --rm -it \
  -v "$PWD/sqlmesh_project:/work" \
  -w /work \
  solo-bloomberg-runtime \
  sqlmesh init duckdb
```
```text
 Sanity Check
  ls -la sqlmesh_project
```

### Step 2 — Confirm SQLMesh can read the project

```bash
docker run --rm -it \
  -v "$PWD/sqlmesh_project:/work" \
  -w /work \
  solo-bloomberg-runtime \
  sqlmesh info
```
```text
When you’re done, paste the output of:  
ls -la sqlmesh_project
```


### Step 3 — Run sqlmesh plan to see what SQLMesh intends to build.

```bash
docker run --rm -it \
  -v "$PWD/sqlmesh_project:/work" \
  -w /work \
  solo-bloomberg-runtime \
  sqlmesh plan
```
```text
When you’re done, paste the output of:  
ls -la sqlmesh_project
```

