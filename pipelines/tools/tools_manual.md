# solo-bloomberg

A self-hosted, containerized analytics platform built with a strict, reproducible runtime.
The system uses Docker as the **only** Python environment—no virtual environments, no host installs.

---

### 

```dockerfile
docker compose exec runtime bash -lc "python -m pipelines.tools.fred_active_scan"
```