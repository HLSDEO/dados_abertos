"""Teste de estrutura do download/8-pncp.py (apenas download, sem parsing)"""

import importlib.util
from pathlib import Path

BASE = Path(__file__).resolve().parents[1] / "etl"
dl_path = BASE / "download" / "8-pncp.py"

spec = importlib.util.spec_from_file_location("dl8", str(dl_path))
dl = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dl)

print("[OK] Modulo download/8-pncp.py carregado")

# Verifica constantes
assert hasattr(dl, "DATA_DIR"), "DATA_DIR ausente"
assert hasattr(dl, "DEFAULT_YEAR"), "DEFAULT_YEAR ausente"
assert dl.DEFAULT_YEAR == "2026", f"DEFAULT_YEAR deveria ser 2026, e não {dl.DEFAULT_YEAR}"
print("[OK] Constantes presentes")

# Verifica funcoes principais (apenas download)
assert hasattr(dl, "_download"), "_download ausente"
assert hasattr(dl, "run"), "run() ausente"
assert hasattr(dl, "_build_urls"), "_build_urls ausente"
print("[OK] Funcoes principais presentes")

# Testa _build_urls
urls = dl._build_urls("2026")
assert "itens" in urls and "contratos" in urls and "empenhos" in urls
assert "2026" in urls["itens"]
print("[OK] _build_urls(2026) OK")

urls2 = dl._build_urls("2025")
assert "2025" in urls2["itens"]
print("[OK] _build_urls(2025) OK")

print("\n[OK] Testes passados")
