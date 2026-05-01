"""
Pipeline 10 - Cartão de Pagamento do Governo Federal (CPGF e CPCC)
"""
import hashlib
import logging
import os
from pathlib import Path

from neo4j import GraphDatabase
from pipeline.lib import wait_for_neo4j, run_batches, iter_csv, IngestionRun, apply_schema, setup_schema, classify_doc

log = logging.getLogger(__name__)

DATA_DIR   = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "cpgf"
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "150000"))
BATCH      = int(os.environ.get("NEO4J_BATCH", "2000"))

# ── Constraints e índices ─────────────────────────────────────────────────────

Q_CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:PortadorCPGF) REQUIRE n.id_portador IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Despesa) REQUIRE n.despesa_id IS UNIQUE",
]

Q_INDEXES = [
    "CREATE INDEX portador_nome  IF NOT EXISTS FOR (n:PortadorCPGF) ON (n.nome)",
    "CREATE INDEX despesa_tipo   IF NOT EXISTS FOR (d:Despesa) ON (d.tipo_despesa)",
]

# ── Queries ───────────────────────────────────────────────────────────────────

# Unidade gestora já existe no grafo (trazida pelo 3-siafi.py ou outros)
# A chave da Despesa será um hash de portador_id + data + valor + favorecido
# Se o favorecido for CNPJ, ligamos à Empresa; se CPF, ligamos à Pessoa

Q_CPGF_UPSERT = """
UNWIND $rows AS r

// 1. Cria ou Atualiza o Portador (só temos o CPF mascarado e o nome)
MERGE (p:PortadorCPGF {id_portador: r.id_portador})
ON CREATE SET p.nome = r.nome_portador,
              p.cpf_mascarado = r.cpf_portador

// 2. Cria a Despesa (usamos nó Despesa para padronizar com Emendas e Câmara)
MERGE (d:Despesa {despesa_id: r.id_despesa})
ON CREATE SET d.tipo_despesa = r.tipo_despesa,
              d.valor_liquido = toFloat(r.valor),
              d.data_emissao = r.data_transacao,
              d.ano = toInteger(r.ano),
              d.mes = toInteger(r.mes),
              d.transacao = r.transacao,
              d.fonte_nome = r.fonte_nome,
              d.cnpj_fornecedor = r.doc_favorecido,
              d.nome_fornecedor = r.nome_favorecido

MERGE (p)-[:REALIZOU]->(d)

// 3. Vincula Unidade Gestora, se houver
WITH r, p, d
WHERE r.codigo_unidade_gestora IS NOT NULL AND trim(r.codigo_unidade_gestora) <> "" AND trim(r.codigo_unidade_gestora) <> "-1"
MERGE (u:UnidadeGestora {cd_uasg: r.codigo_unidade_gestora})
  ON CREATE SET u.nome = r.nome_unidade_gestora, u.no_uasg = r.nome_unidade_gestora
MERGE (p)-[:LOTADO_EM]->(u)
MERGE (u)-[:CUSTEIA]->(d)
"""

Q_CPGF_FAVORECIDO = """
UNWIND $rows AS r
MATCH (d:Despesa {despesa_id: r.id_despesa})

CALL {
    WITH r, d
    WITH r, d WHERE r.doc_tipo = 'cnpj_valid'
    // Favorecido Empresa
    MERGE (e:Empresa {cnpj_basico: substring(r.doc_favorecido, 0, 8)})
      ON CREATE SET e.razao_social = r.nome_favorecido
    MERGE (d)-[:PAGO_A]->(e)
    RETURN count(e) AS c1

    UNION

    WITH r, d
    WITH r, d WHERE r.doc_tipo = 'cpf_valid'
    // Favorecido Pessoa Física
    MERGE (pe:Pessoa {cpf: r.doc_favorecido})
      ON CREATE SET pe.nome = r.nome_favorecido
    MERGE (d)-[:PAGO_A]->(pe)
    RETURN count(pe) AS c1
}
RETURN sum(c1) AS total_links
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def _hash_id(*parts) -> str:
    s = "|".join(str(p).strip().upper() for p in parts)
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def _parse_valor(v: str) -> float:
    if not v: return 0.0
    return float(v.replace(".", "").replace(",", ".").strip() or 0.0)

def _parse_date(d: str) -> str:
    # d é DD/MM/YYYY, converte para YYYY-MM-DD
    if not d or len(d) != 10: return ""
    return f"{d[6:10]}-{d[3:5]}-{d[0:2]}"

# ── Loaders ───────────────────────────────────────────────────────────────────

def _process_cpgf(driver, csv_path: Path):
    if not csv_path.exists():
        log.warning(f"  Arquivo não encontrado: {csv_path}")
        return 0

    log.info(f"  Processando {csv_path.name}...")
    total = 0

    with driver.session() as session:
        for chunk in iter_csv(csv_path, delimiter=";", encoding="latin-1"): # A fonte é latin-1 no download
            rows = []
            favorecidos = []
            
            for row in chunk:
                # O iter_csv pode ler o dict usando chaves uppercase baseadas no header
                # Exemplo de headers do CPGF: 
                # CÓDIGO ÓRGÃO SUPERIOR, NOME ÓRGÃO SUPERIOR, CÓDIGO ÓRGÃO, NOME ÓRGÃO, 
                # CÓDIGO UNIDADE GESTORA, NOME UNIDADE GESTORA, ANO EXTRATO, MÊS EXTRATO, 
                # CPF PORTADOR, NOME PORTADOR, CNPJ OU CPF FAVORECIDO, NOME FAVORECIDO, 
                # TRANSAÇÃO, DATA TRANSAÇÃO, VALOR TRANSAÇÃO

                doc_favorecido = row.get("CNPJ OU CPF FAVORECIDO", "").replace(".", "").replace("-", "").replace("/", "").strip()
                nome_favorecido = row.get("NOME FAVORECIDO", "").strip()
                cpf_portador = row.get("CPF PORTADOR", "").strip()
                nome_portador = row.get("NOME PORTADOR", "").strip()
                data_trans = _parse_date(row.get("DATA TRANSAÇÃO", ""))
                valor = _parse_valor(row.get("VALOR TRANSAÇÃO", ""))
                transacao = row.get("TRANSAÇÃO", "").strip()
                uasg = row.get("CÓDIGO UNIDADE GESTORA", "").strip()
                nome_uasg = row.get("NOME UNIDADE GESTORA", "").strip()
                
                # Ignorar linhas de saque ou sem portador definido (pode ser SIGILOSO)
                if not cpf_portador or cpf_portador.upper() == "SIGILOSO":
                    continue
                
                portador_id = _hash_id(nome_portador, cpf_portador)
                despesa_id = _hash_id(portador_id, data_trans, str(valor), doc_favorecido, transacao)

                rows.append({
                    "id_portador": portador_id,
                    "cpf_portador": cpf_portador,
                    "nome_portador": nome_portador,
                    "id_despesa": despesa_id,
                    "tipo_despesa": "CARTAO CORPORATIVO (CPGF)",
                    "valor": valor,
                    "data_transacao": data_trans,
                    "ano": row.get("ANO EXTRATO", "0"),
                    "mes": row.get("MÊS EXTRATO", "0"),
                    "transacao": transacao,
                    "codigo_unidade_gestora": uasg,
                    "nome_unidade_gestora": nome_uasg,
                    "doc_favorecido": doc_favorecido,
                    "nome_favorecido": nome_favorecido,
                    "fonte_nome": row.get("fonte_nome", "CGU — Portal da Transparência")
                })
                
                if doc_favorecido and doc_favorecido not in ("-1", "-2", "SIGILOSO", ""):
                    doc_tipo = classify_doc(doc_favorecido)
                    if doc_tipo in ("cnpj_valid", "cpf_valid"):
                        favorecidos.append({
                            "id_despesa": despesa_id,
                            "doc_favorecido": doc_favorecido,
                            "nome_favorecido": nome_favorecido,
                            "doc_tipo": doc_tipo
                        })

            if rows:
                run_batches(session, Q_CPGF_UPSERT, rows)
            if favorecidos:
                run_batches(session, Q_CPGF_FAVORECIDO, favorecidos)

            total += len(rows)

    return total


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str):
    log.info(f"[cpgf] Pipeline  chunk={CHUNK_SIZE:,}  batch={BATCH}")

    if not DATA_DIR.exists():
        log.warning(f"  Diretório não existe: {DATA_DIR} — rode 'download cpgf'")
        return

    cpgf_csv = DATA_DIR / "cpgf.csv"
    cpcc_csv = DATA_DIR / "cpcc.csv" # Compras centralizadas

    driver = wait_for_neo4j(neo4j_uri, neo4j_user, neo4j_password)
    setup_schema(driver)

    with driver.session() as session:
        log.info("  Constraints e índices...")
        apply_schema(session, Q_CONSTRAINTS, Q_INDEXES)

    with IngestionRun(driver, "cpgf") as run_ctx:
        n1 = _process_cpgf(driver, cpgf_csv)
        # O CPCC pode ter headers ligeiramente diferentes se analisarmos a fundo, 
        # mas caso sejam similares, podemos processar também. 
        # O código de extração atual ignora as discrepâncias e tenta mapear as colunas comuns.
        
        # n2 = _process_cpgf(driver, cpcc_csv)
        # run_ctx.add(rows_in=n1+n2, rows_out=n1+n2)
        
        run_ctx.add(rows_in=n1, rows_out=n1)
        log.info(f"  ✓ {n1:,} despesas CPGF carregadas no total.")

    driver.close()
    log.info("[cpgf] Pipeline concluído")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%H:%M:%S",
    )
    run(
        neo4j_uri=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        neo4j_user=os.environ.get("NEO4J_USER", "neo4j"),
        neo4j_password=os.environ.get("NEO4J_PASSWORD", "senha"),
    )
