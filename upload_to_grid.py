#!/usr/bin/env python3
"""
Sube dashboard_nps_full.html a Grid.
Requiere VPN corporativa MELI activa.

Uso manual:  python upload_to_grid.py
Task Scheduler: configurar para que corra ~10 min después de las 11:00 UTC
(esperar que GitHub Actions termine el refresh y pushee el HTML actualizado).
"""

import json, subprocess, sys, logging
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: pip install requests")
    sys.exit(1)

SCRIPT_DIR = Path(__file__).parent
HTML_FILE  = SCRIPT_DIR / "dashboard_nps_full.html"
LOG_FILE   = SCRIPT_DIR / "upload.log"
GRID_API   = "https://grid.melioffice.com/api/v1/engine/run"
DOC_ID     = "01KPVNJNMB6CA09M9YZW98TH83"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("kta_upload")


def pull_latest():
    """git pull para traer el HTML actualizado por GitHub Actions."""
    log.info("git pull ...")
    result = subprocess.run(
        ["git", "pull", "--rebase"],
        capture_output=True, text=True, cwd=SCRIPT_DIR
    )
    if result.returncode != 0:
        log.warning(f"git pull falló (puede no ser fatal): {result.stderr.strip()}")
    else:
        log.info(result.stdout.strip() or "ya al dia")


def upload():
    if not HTML_FILE.exists():
        log.error(f"HTML no encontrado: {HTML_FILE}")
        sys.exit(1)

    size = HTML_FILE.stat().st_size
    log.info(f"Subiendo {HTML_FILE.name} ({size:,} bytes) → Grid {DOC_ID} ...")

    config = {
        "skill_version": "3.6.0",
        "doc_id": DOC_ID,
        "skip_version_check": True,
    }

    with open(HTML_FILE, "rb") as fh:
        resp = requests.post(
            GRID_API,
            data={"config": json.dumps(config)},
            files={"file": (HTML_FILE.name, fh, "text/html")},
            timeout=90,
        )

    try:
        data = resp.json()
    except Exception:
        log.error(f"Respuesta no-JSON (status {resp.status_code}): {resp.text[:300]}")
        sys.exit(1)

    steps = data.get("steps", [])
    file_ok = any(
        s.get("label") in ("file_replaced", "uploaded", "version_uploaded")
        and s.get("status") == "OK"
        for s in steps
    )

    if data.get("ok") or file_ok:
        view_url = data.get("view_url", "—")
        log.info(f"OK → {view_url}")
    else:
        log.error(f"Grid rechazó la subida: {data}")
        if resp.status_code == 401:
            log.error("HTTP 401 — verificar que la VPN corporativa MELI esté activa.")
        sys.exit(1)


if __name__ == "__main__":
    pull_latest()
    upload()
