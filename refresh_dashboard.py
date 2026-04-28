#!/usr/bin/env python3
"""
KTA Brasil — Dashboard refresh para GitHub Actions
Corre las queries de BigQuery y actualiza dashboard_nps_full.html en el repo.
No sube a Grid (requiere VPN — usar upload_to_grid.py localmente).

Auth BigQuery en GitHub Actions: secreto GCP_SA_KEY (JSON de service account).
Auth BigQuery local: gcloud auth application-default login
"""

import json, re, os, sys, base64, logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    from google.cloud import bigquery
except ImportError:
    print("ERROR: pip install google-cloud-bigquery")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
HTML_FILE  = SCRIPT_DIR / "dashboard_nps_full.html"
LOG_FILE   = SCRIPT_DIR / "refresh.log"
PROJECT_ID = "meli-bi-data"

TEAMS = [
    "BR_ME_Sellers_Longtail",
    "BR_Publicaciones_Sellers_Longtail",
    "BR_Ventas_Sellers_Longtail",
    "BR_MLVendedor_Analisis_Masivo",
]

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("kta_refresh")

# ── BigQuery auth — service account JSON desde env var (GitHub Actions) ──────
def get_bq_client():
    sa_json = os.environ.get("GCP_SA_KEY", "")
    if sa_json:
        import tempfile
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        tmp.write(sa_json)
        tmp.close()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp.name
        log.info("  Auth: service account desde GCP_SA_KEY")
    else:
        log.info("  Auth: gcloud application-default (local)")
    return bigquery.Client(project=PROJECT_ID)

# ── Queries ───────────────────────────────────────────────────────────────────
SQL_NPS_MONTHLY = """
SELECT
  USER_TEAM_NAME                                                      AS equipe,
  COUNT(*)                                                            AS enc,
  COUNTIF(NPS > 0)                                                    AS pro,
  COUNTIF(NPS = 0)                                                    AS neu,
  COUNTIF(NPS < 0)                                                    AS det,
  ROUND((COUNTIF(NPS > 0) - COUNTIF(NPS < 0)) * 100.0 / COUNT(*), 1) AS nps_score
FROM `meli-bi-data.WHOWNER.DM_CX_KM_NEW_STORYTELLING_NPS`
WHERE USER_TEAM_NAME IN UNNEST(@teams)
  AND USER_LDAP_KM_SEGMENT_GROUP IN ('NEWBIE')
  AND FLAG_TARGET_TEAM_CHANNEL = TRUE
  AND USER_OFFICE = 'KTA'
  AND DATE(DATE_ID) >= DATE_TRUNC(CURRENT_DATE('America/Bogota'), MONTH)
GROUP BY USER_TEAM_NAME
ORDER BY USER_TEAM_NAME
"""

SQL_NPS_WEEKLY = """
SELECT
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(DATE(DATE_ID), WEEK(MONDAY))) AS s,
  USER_TEAM_NAME                                                     AS equipe,
  COUNT(*)                                                           AS enc,
  ROUND(AVG(NPS), 1)                                                 AS NPS
FROM `meli-bi-data.WHOWNER.DM_CX_KM_NEW_STORYTELLING_NPS`
WHERE USER_TEAM_NAME IN UNNEST(@teams)
  AND USER_LDAP_KM_SEGMENT_GROUP IN ('NEWBIE')
  AND FLAG_TARGET_TEAM_CHANNEL = TRUE
  AND USER_OFFICE = 'KTA'
  AND DATE(DATE_ID) >= DATE_SUB(
        DATE_TRUNC(CURRENT_DATE('America/Bogota'), MONTH), INTERVAL 4 WEEK)
GROUP BY 1, 2
ORDER BY 1, 2
"""

SQL_NPS_PROCESSOS = """
SELECT
  USER_TEAM_NAME   AS equipe,
  PRO_PROCESS_NAME AS proc,
  COUNT(*)         AS enc,
  ROUND((COUNTIF(NPS > 0) - COUNTIF(NPS < 0)) * 100.0 / COUNT(*), 1) AS NPS,
  ROUND(AVG(SAFE_CAST(SURVEY_TARGET_VALUE AS FLOAT64)) * 100, 1)      AS target,
  ROUND(COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (PARTITION BY USER_TEAM_NAME), 1) AS pct
FROM `meli-bi-data.WHOWNER.DM_CX_KM_NEW_STORYTELLING_NPS`
WHERE USER_TEAM_NAME IN UNNEST(@teams)
  AND USER_LDAP_KM_SEGMENT_GROUP IN ('NEWBIE')
  AND FLAG_TARGET_TEAM_CHANNEL = TRUE
  AND USER_OFFICE = 'KTA'
  AND DATE(DATE_ID) >= DATE_TRUNC(CURRENT_DATE('America/Bogota'), MONTH)
GROUP BY 1, 2
ORDER BY 1, enc DESC
"""

SQL_EM = """
SELECT
  USER_TEAM_NAME AS equipe,
  CASE
    WHEN USER_TEAM_CHANNEL IN ('C2C','MULTICANAL C2C')   THEN 'C2C'
    WHEN USER_TEAM_CHANNEL IN ('CHAT','MULTICANAL CHAT') THEN 'CHAT'
    ELSE USER_TEAM_CHANNEL
  END                        AS canal,
  COUNT(*)                   AS total,
  ROUND(AVG(QM_TOTAL) * 100, 1) AS pct,
  ROUND(AVG(QM_TOTAL) * 2,   2) AS nota
FROM `meli-bi-data.WHOWNER.DM_CX_QM_ANALYSIS_METRIC_REASON`
WHERE ANALYSIS_REASON = 'Métrica Oficial'
  AND METRIC_TYPE     = 'Métrica Oficial'
  AND DATE(ASSIGN_DATE) >= DATE_TRUNC(CURRENT_DATE('America/Bogota'), MONTH)
  AND USER_TEAM_NAME IN UNNEST(@teams)
GROUP BY 1, 2
ORDER BY 1, 2
"""

SQL_TRAINING = """
SELECT
  A.USER_TEAM_NAME AS USER_TEAM_NAME,
  COUNT(DISTINCT A.USER_LDAP) AS agentes,
  ROUND(SAFE_DIVIDE(
    SUM(TRAINING_AC_TIME),
    SUM(ONLINE_TIME_SEC) + SUM(ONLINE_TIME_LOWP_SEC) + SUM(HELP_EXCLUSIVE_TIME) +
    SUM(SHADOWING_TIME)  + SUM(EMAIL_TIME)            + SUM(POST_CONTACT_TIME)   +
    SUM(BREAK_TIME_SEC)  + SUM(COACHING_TIME_SEC)     + SUM(EVENT_TIME_SEC)      +
    SUM(TRAINING_AC_TIME)+ SUM(TRAINING_KM_TIME)      + SUM(EXPERT_TRAINER_TIME_SEC)+
    SUM(LEARNING_TIME)   + SUM(TEACHING_TIME)         + SUM(NESTING_TIME)        +
    SUM(OPERATIONAL_FAILURE_TIME_SEC) + SUM(SYSTEMATIC_FAILURE_TIME)
  ) * 100, 2) AS pct,
  ROUND(SUM(TRAINING_AC_TIME) / 3600, 2) AS hTrain,
  ROUND((
    SUM(ONLINE_TIME_SEC) + SUM(ONLINE_TIME_LOWP_SEC) + SUM(HELP_EXCLUSIVE_TIME) +
    SUM(SHADOWING_TIME)  + SUM(EMAIL_TIME)            + SUM(POST_CONTACT_TIME)   +
    SUM(BREAK_TIME_SEC)  + SUM(COACHING_TIME_SEC)     + SUM(EVENT_TIME_SEC)      +
    SUM(TRAINING_AC_TIME)+ SUM(TRAINING_KM_TIME)      + SUM(EXPERT_TRAINER_TIME_SEC)+
    SUM(LEARNING_TIME)   + SUM(TEACHING_TIME)         + SUM(NESTING_TIME)        +
    SUM(OPERATIONAL_FAILURE_TIME_SEC) + SUM(SYSTEMATIC_FAILURE_TIME)
  ) / 3600, 2) AS hLog
FROM `meli-bi-data.WHOWNER.BT_CX_REP_METRICS_CS` A
LEFT JOIN `meli-bi-data.WHOWNER.BT_CX_KM_TRAINING_STATUS` B
  ON A.USER_LDAP = B.USER_LDAP AND A.DATE_ID = B.DATE_ID
WHERE DATE_TRUNC(A.DATE_ID, MONTH) = DATE_TRUNC(CURRENT_DATE('America/Bogota'), MONTH)
  AND A.USER_ROLE IN ('AGENT')
  AND (B.KM_STATUS IS NULL OR B.KM_STATUS <> 'TRAINING')
  AND A.USER_TEAM_NAME IN UNNEST(@teams)
  AND A.USER_LDAP IN (
    SELECT DISTINCT USER_LDAP
    FROM `meli-bi-data.WHOWNER.DM_CX_KM_NEW_STORYTELLING_NPS`
    WHERE USER_TEAM_NAME IN UNNEST(@teams)
      AND USER_LDAP_KM_SEGMENT_GROUP = 'NEWBIE'
      AND USER_OFFICE = 'KTA'
  )
GROUP BY A.USER_TEAM_NAME
ORDER BY A.USER_TEAM_NAME
"""

SQL_EM_SENTENCES = """
SELECT
  r.USER_TEAM_NAME                                                       AS equipe,
  r.ANALYSIS_SENTENCE                                                    AS sentencia,
  CASE
    WHEN r.ANALYSIS_MOMENT IN ('INICIO DEL CONTACTO','INICIO_DE_CONTACTO') THEN '1-INICIO'
    WHEN r.ANALYSIS_MOMENT IN ('EXPLORACIÓN','EXPLORACION')            THEN '2-EXPLORACION'
    WHEN r.ANALYSIS_MOMENT IN ('GUIA Y ASESORAMIENTO','GUIA_Y_ASESORAMIENTO') THEN '3-GUIA'
    WHEN r.ANALYSIS_MOMENT IN ('CIERRE DE EXPERIENCIA','CIERRE_DE_EXPERIENCIA') THEN '4-CIERRE'
    WHEN r.ANALYSIS_MOMENT IN ('COMO')                                    THEN '5-COMO'
    ELSE NULL
  END                                                                    AS momento,
  COUNT(*)                                                               AS total_casos,
  COUNTIF(r.ANALYSIS_SENTENCE_VALUE IN ('Bajo','BAJO','Medio-bajo','MEDIO BAJO')) AS cnt_bajo
FROM `meli-bi-data.WHOWNER.DM_CX_QM_ANALYSIS_METRIC_REASON` r
WHERE r.USER_TEAM_NAME IN UNNEST(@teams)
  AND r.ANALYSIS_REASON = 'Métrica Oficial'
  AND r.METRIC_TYPE     = 'Métrica Oficial'
  AND r.ASSIGN_DATE >= DATE_TRUNC(CURRENT_DATE('America/Bogota'), MONTH)
  AND r.ANALYSIS_SENTENCE IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1, cnt_bajo DESC
"""

SQL_MATRIZ_FALTAS = """
SELECT
  R.USER_TEAM_NAME  AS equipe,
  R.PDC_NAME        AS tipo_falta,
  R.PICKLIST_NAME   AS falta,
  COUNT(*)          AS total
FROM `meli-bi-data.WHOWNER.DM_CX_QM_METRIC_REASONS` R
WHERE R.FLAG_VALID_ANALYSIS IS TRUE
  AND R.FLAG_NOT_DUPLICATED IS TRUE
  AND R.ANALYSIS_REASON = 'QI Metric'
  AND LOWER(CAST(R.PICKLIST_VALUE AS STRING)) = 'true'
  AND R.QM_INAPPROPRIATE_BEHAVIOR = 0
  AND R.REFERENCE_DATE >= DATE_TRUNC(CURRENT_DATE('America/Bogota'), MONTH)
  AND R.USER_TEAM_NAME IN UNNEST(@teams)
GROUP BY 1, 2, 3
ORDER BY 1, total DESC
"""

SQL_CDU_DETAIL = """
SELECT
  USER_TEAM_NAME                                                          AS equipe,
  PRO_PROCESS_NAME                                                        AS proc,
  CDU                                                                     AS cdu,
  COUNT(*)                                                                AS enc,
  ROUND((COUNTIF(NPS > 0) - COUNTIF(NPS < 0)) * 100.0 / COUNT(*), 1)    AS nps_score,
  ROUND(AVG(SAFE_CAST(SURVEY_TARGET_VALUE AS FLOAT64)) * 100, 1)         AS target,
  ROUND(COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (PARTITION BY USER_TEAM_NAME, PRO_PROCESS_NAME), 1) AS pct_in_proc,
  COUNT(DISTINCT TRAINING_ID)                                             AS turmas
FROM `meli-bi-data.WHOWNER.DM_CX_KM_NEW_STORYTELLING_NPS`
WHERE USER_TEAM_NAME IN UNNEST(@teams)
  AND USER_LDAP_KM_SEGMENT_GROUP IN ('NEWBIE')
  AND FLAG_TARGET_TEAM_CHANNEL = TRUE
  AND USER_OFFICE = 'KTA'
  AND DATE(DATE_ID) >= DATE_TRUNC(CURRENT_DATE('America/Bogota'), MONTH)
  AND CDU IS NOT NULL AND TRIM(CDU) != ''
GROUP BY 1, 2, 3
HAVING COUNT(*) >= 3
ORDER BY 1, 2, COUNT(*) DESC
"""

SQL_REP_DETAIL = """
WITH nps_por_rep AS (
  SELECT
    s.USER_TEAM_NAME AS equipe,
    s.USER_LDAP      AS ldap,
    CASE
      WHEN s.USER_TEAM_CHANNEL IN ('C2C','MULTICANAL C2C')   THEN 'C2C'
      WHEN s.USER_TEAM_CHANNEL IN ('CHAT','MULTICANAL CHAT') THEN 'Chat'
      ELSE s.USER_TEAM_CHANNEL
    END                                                                      AS canal,
    COUNT(*)                                                                 AS enc,
    ROUND((COUNTIF(s.NPS > 0) - COUNTIF(s.NPS < 0)) * 100.0 / COUNT(*), 1) AS nps,
    ROUND(AVG(SAFE_CAST(s.SURVEY_TARGET_VALUE AS FLOAT64)) * 100, 1)        AS target
  FROM `meli-bi-data.WHOWNER.DM_CX_KM_NEW_STORYTELLING_NPS` s
  WHERE s.USER_TEAM_NAME             IN UNNEST(@teams)
    AND s.USER_LDAP_KM_SEGMENT_GROUP  = 'NEWBIE'
    AND s.FLAG_TARGET_TEAM_CHANNEL    = TRUE
    AND s.USER_OFFICE                 = 'KTA'
    AND DATE(s.DATE_ID) >= DATE_TRUNC(CURRENT_DATE('America/Bogota'), MONTH)
  GROUP BY 1, 2, 3
),
training_pct AS (
  SELECT
    A.USER_TEAM_NAME,
    A.USER_LDAP,
    ROUND(SAFE_DIVIDE(
      SUM(TRAINING_AC_TIME),
      SUM(ONLINE_TIME_SEC) + SUM(ONLINE_TIME_LOWP_SEC) + SUM(HELP_EXCLUSIVE_TIME) +
      SUM(SHADOWING_TIME)  + SUM(EMAIL_TIME)            + SUM(POST_CONTACT_TIME)   +
      SUM(BREAK_TIME_SEC)  + SUM(COACHING_TIME_SEC)     + SUM(EVENT_TIME_SEC)      +
      SUM(TRAINING_AC_TIME)+ SUM(TRAINING_KM_TIME)      + SUM(EXPERT_TRAINER_TIME_SEC)+
      SUM(LEARNING_TIME)   + SUM(TEACHING_TIME)         + SUM(NESTING_TIME)        +
      SUM(OPERATIONAL_FAILURE_TIME_SEC) + SUM(SYSTEMATIC_FAILURE_TIME)
    ) * 100, 1) AS pct_train,
    ROUND(SUM(TRAINING_AC_TIME) / 3600, 1) AS h_train
  FROM `meli-bi-data.WHOWNER.BT_CX_REP_METRICS_CS` A
  WHERE DATE_TRUNC(A.DATE_ID, MONTH) = DATE_TRUNC(CURRENT_DATE('America/Bogota'), MONTH)
    AND A.USER_ROLE       = 'AGENT'
    AND A.USER_TEAM_NAME  IN UNNEST(@teams)
  GROUP BY 1, 2
)
SELECT
  n.equipe,
  n.ldap,
  n.canal,
  n.enc,
  n.nps,
  COALESCE(n.target, 69.5)   AS target,
  COALESCE(t.pct_train, 0.0) AS pct_train,
  COALESCE(t.h_train,   0.0) AS h_train
FROM nps_por_rep n
LEFT JOIN training_pct t ON n.equipe = t.USER_TEAM_NAME AND n.ldap = t.USER_LDAP
ORDER BY n.equipe, n.nps DESC
"""

# ── EM metadata ───────────────────────────────────────────────────────────────
_EM_LABELS = {
    "exploracion_repasa_situacion_del_usuario":
        "Exploracao - Repassa situacao do usuario",
    "guia_asesoramiento_asegura_comprension_de_forma_eficaz":
        "Guia - Assegura compreensao de forma eficaz",
    "exploracion_escucha_activamente":
        "Exploracao - Escuta ativamente",
    "guia_asesoramiento_comunica_las_soluciones_de_manera_clara":
        "Guia - Comunica solucoes de forma clara",
    "como_silencio":
        "Como - Silencio excessivo",
    "exploracion_realiza_preguntas_y_confirmaciones":
        "Exploracao - Realiza perguntas e confirmacoes",
    "inicio_contacto_saludo_y_presentacion_inicial":
        "Inicio - Saudacao e apresentacao inicial",
    "inicio_contacto_ponerse_a_disposicion_con_contexto":
        "Inicio - Disposicao com contexto",
    "cierre_permaneciendo_a_disposicion":
        "Cierre - Permanece a disposicao",
    "como_tono":
        "Como - Tom de voz",
    "como_velocidad":
        "Como - Velocidade",
}

_EM_DINAMICA = {
    "exploracion_repasa_situacion_del_usuario": {
        "titulo": "O rep entendeu antes de responder?",
        "duracao": "30 min - Teorica + Pratica",
        "objetivo": "O rep aprende a repassar a situacao do usuario ANTES de navegar no fluxo, evitando caminhos incorretos.",
        "como": (
            "Parte 1 (10 min) - Diagnostico: apresentar 3 casos reais onde o rep avancou sem repassar. "
            "A turma identifica o momento exato em que o rep deveria ter parado. "
            "Parte 2 (15 min) - Roleplay: um rep faz o papel do usuario, o outro pratica a frase de repasse obrigatoria: "
            "'Para eu te ajudar melhor, quero confirmar o que esta acontecendo: [situacao]?' "
            "ANTES de qualquer solucao. "
            "Parte 3 (5 min) - Debriefing: mostre como a ausencia de repasse amplifica o erro de fluxo e gera NPS baixo."
        ),
        "materiais": "3 casos reais do periodo exportados do sistema.",
        "tags": ["Exploracao", "Fluxo"]
    },
    "guia_asesoramiento_asegura_comprension_de_forma_eficaz": {
        "titulo": "O usuario entendeu ou so ouviu?",
        "duracao": "25 min - Pratica",
        "objetivo": "O rep aprende a confirmar que o usuario compreendeu a solucao antes de fechar o contato.",
        "como": (
            "Parte 1 (10 min) - Contraste: mostrar 2 gravacoes - uma sem confirmacao, outra com. "
            "Parte 2 (15 min) - Roleplay em duplas: ao final de cada resposta o rep deve usar uma frase de confirmacao."
        ),
        "materiais": "Gravacoes reais do periodo (1 boa pratica, 1 oportunidade).",
        "tags": ["Guia", "Estilo Meli - Comprensao"]
    },
    "exploracion_escucha_activamente": {
        "titulo": "Estou ouvindo ou esperando para falar?",
        "duracao": "30 min - Analise de caso + Roleplay",
        "objetivo": "O rep pratica escuta ativa: nao interrompe, registra informacao chave e confirma antes de responder.",
        "como": (
            "Parte 1 (10 min) - Analise: apresentar caso onde o rep interrompeu o usuario. "
            "Parte 2 (15 min) - Roleplay: usuario descreve situacao complexa, o rep escuta sem interromper e resume. "
            "Parte 3 (5 min) - Feedback: o usuario avalia se se sentiu ouvido."
        ),
        "materiais": "Casos reais do periodo. Formulario de escuta ativa.",
        "tags": ["Exploracao", "Estilo Meli - Escuta"]
    },
    "guia_asesoramiento_comunica_las_soluciones_de_manera_clara": {
        "titulo": "A solucao foi dada ou foi explicada?",
        "duracao": "25 min - Pratica",
        "objetivo": "O rep aprende a comunicar solucoes com clareza: o que vai acontecer, em que prazo e qual e o proximo passo.",
        "como": (
            "Parte 1 (10 min) - Contraste: ler duas respostas ao mesmo caso - uma tecnica sem clareza, outra estruturada. "
            "Parte 2 (15 min) - Pratica escrita: cada rep reescreve a resposta usando a estrutura: "
            "[O que vai acontecer] + [Em que prazo] + [O que o usuario precisa fazer]."
        ),
        "materiais": "2 exemplos de resposta. Template de estrutura de solucao clara.",
        "tags": ["Guia", "Comunicacao"]
    },
    "como_silencio": {
        "titulo": "O silencio tem que falar por voce",
        "duracao": "20 min - Simulacao",
        "objetivo": "O rep aprende a manter o usuario informado durante verificacoes, eliminando silencios nao comunicados.",
        "como": (
            "Parte 1 (10 min) - Simulacao com cronometro: silencio sem aviso vs com frases de manutencao. "
            "Parte 2 (10 min) - Pratica em duplas com frases aprovadas a cada 20s."
        ),
        "materiais": "Cronometro. Lista de frases aprovadas de manutencao de contato.",
        "tags": ["Como", "Silencio", "Estilo Meli"]
    },
}

_EM_ERRO = {
    "exploracion_repasa_situacion_del_usuario":
        "Rep navega no fluxo sem repassar a situacao do usuario — gera solucao incorreta e recontato.",
    "guia_asesoramiento_asegura_comprension_de_forma_eficaz":
        "Rep fecha o contato sem confirmar que o usuario compreendeu — gera insatisfacao e recontato.",
    "exploracion_escucha_activamente":
        "Rep interrompe ou nao registra informacoes chave — perde contexto e oferece solucao inadequada.",
    "guia_asesoramiento_comunica_las_soluciones_de_manera_clara":
        "Rep comunica a solucao sem estrutura clara — usuario sai sem saber o que fazer a seguir.",
    "como_silencio":
        "Rep deixa silencio prolongado sem comunicar — usuario percebe abandono e desconforca.",
}

NPS_TGT_DEFAULT = 69.5

# ── Helpers ───────────────────────────────────────────────────────────────────
def run_query(client, sql, label):
    import time
    from google.api_core.exceptions import Forbidden, ServiceUnavailable
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("teams", "STRING", TEAMS)]
    )
    for attempt in range(1, 5):
        try:
            log.info(f"  Query: {label} ...")
            rows = list(client.query(sql, job_config=job_config).result())
            log.info(f"  {label}: {len(rows)} filas")
            time.sleep(3)
            return [dict(r.items()) for r in rows]
        except (Forbidden, ServiceUnavailable) as e:
            if attempt < 4:
                wait = 15 * attempt
                log.warning(f"  {label}: reintento {attempt}/3 en {wait}s ...")
                time.sleep(wait)
            else:
                log.error(f"  {label}: fallido tras 4 intentos")
                raise


def to_safe_js(var_name, data):
    j = json.dumps(data, ensure_ascii=True,
                   default=lambda x: float(x) if hasattr(x, "__float__") else str(x))
    b64 = base64.b64encode(j.encode("ascii")).decode("ascii")
    return f'const {var_name} = JSON.parse(atob("{b64}"));'


def inject_or_replace(html, marker, var_name, new_js_line):
    # Estrategia 1 — marcadores __MARKER_START/END__
    pattern = rf"// __{marker}_START__.*?// __{marker}_END__"
    result, n = re.subn(
        pattern,
        lambda m: f"// __{marker}_START__\n{new_js_line}\n// __{marker}_END__",
        html, flags=re.DOTALL
    )
    if n > 0:
        log.info(f"  {var_name}: via marcadores")
        return result

    # Estrategia 2 — declaracion base64 previa
    result, n = re.subn(
        rf'const {re.escape(var_name)} = JSON\.parse\(atob\("[A-Za-z0-9+/=]*"\)\);',
        new_js_line, html
    )
    if n > 0:
        log.info(f"  {var_name}: base64 reemplazada")
        return result

    # Estrategia 3 — parser de brackets para declaracion literal
    for kw in ("const ", "let ", "var "):
        idx = html.find(f"{kw}{var_name} =")
        if idx == -1:
            idx = html.find(f"{kw}{var_name}=")
        if idx == -1:
            continue
        val_start = html.index("=", idx) + 1
        while val_start < len(html) and html[val_start] in " \t\n":
            val_start += 1
        if val_start >= len(html):
            break
        open_ch = html[val_start]
        if open_ch not in ("{", "[", "("):
            break
        close_ch = {"{": "}", "[": "]", "(": ")"}[open_ch]
        depth, pos, in_str, str_ch = 0, val_start, False, ""
        end_pos = -1
        while pos < len(html):
            c = html[pos]
            if in_str:
                if c == str_ch and (pos == 0 or html[pos - 1] != "\\"):
                    in_str = False
            elif c in ('"', "'", "`"):
                in_str, str_ch = True, c
            elif c == open_ch:
                depth += 1
            elif c == close_ch:
                depth -= 1
                if depth == 0:
                    end_pos = pos + 1
                    if end_pos < len(html) and html[end_pos] == ";":
                        end_pos += 1
                    break
            pos += 1
        if end_pos > 0:
            log.info(f"  {var_name}: declaracion literal reemplazada")
            return html[:idx] + new_js_line + html[end_pos:]
        break

    log.warning(f"  {var_name}: no se encontro punto de inyeccion")
    return html


def build_cdu_analysis(nps_processos, cdu_detail, em_sentences, matriz_faltas):
    from collections import defaultdict

    proc_by_team = defaultdict(list)
    for r in nps_processos:
        proc_by_team[r["equipe"]].append(r)

    cdus_by_team_proc = defaultdict(list)
    for r in cdu_detail:
        cdus_by_team_proc[(r["equipe"], r["proc"])].append(r)

    em_by_team = defaultdict(list)
    for r in em_sentences:
        em_by_team[r["equipe"]].append(r)

    faltas_by_team = defaultdict(list)
    for r in matriz_faltas:
        faltas_by_team[r["equipe"]].append(r)

    din_base = {
        "titulo": "Prioridade identificada — dinamica em elaboracao",
        "duracao": "30 min",
        "objetivo": "Melhorar performance no CDU identificado.",
        "como": "-",
        "materiais": "Casos reais do periodo.",
        "tags": []
    }

    result = {}
    for team, procs in proc_by_team.items():
        below = [p for p in procs if float(p["NPS"]) < float(p["target"] or NPS_TGT_DEFAULT)]
        crit = sorted(below or procs, key=lambda p: (-float(p["enc"]), float(p["NPS"])))[0]
        crit_proc = crit["proc"]
        crit_nps  = float(crit["NPS"])
        crit_tgt  = float(crit["target"] or NPS_TGT_DEFAULT)
        crit_enc  = int(crit["enc"])
        crit_pct  = float(crit["pct"])
        crit_gap  = round(crit_nps - crit_tgt, 1)

        team_em = sorted(em_by_team[team], key=lambda r: -int(r["cnt_bajo"]))
        em_sorted = [r for r in team_em if int(r["cnt_bajo"]) > 0][:5]

        raw_cdus = cdus_by_team_proc.get((team, crit_proc), [])

        def _below(r):
            tgt = float(r["target"]) if r.get("target") is not None else NPS_TGT_DEFAULT
            return float(r["nps_score"]) < tgt and int(r["enc"]) >= 3

        def _sort(r):
            tgt = float(r["target"]) if r.get("target") is not None else NPS_TGT_DEFAULT
            return (-int(r["enc"]), -(tgt - float(r["nps_score"])))

        cdus_b = sorted([c for c in raw_cdus if _below(c)], key=_sort)
        cdus_a = sorted([c for c in raw_cdus if not _below(c)], key=lambda r: -int(r["enc"]))
        top_cdus = (cdus_b + cdus_a)[:3]

        prios = ["ALTA", "ALTA", "MÉDIA"]
        cdu_items = []
        for i, c in enumerate(top_cdus):
            tgt  = float(c["target"]) if c.get("target") is not None else NPS_TGT_DEFAULT
            gap  = round(float(c["nps_score"]) - tgt, 1)
            enc  = int(c["enc"])
            trms = int(c.get("turmas") or 0)
            nome = c["cdu"]
            porque = (
                f"CDU com maior recorrencia: {trms} turma(s), {enc} surveys. "
                f"GAP de {abs(gap)} p.p. vs target de {tgt}%."
            ) if i == 0 else (
                f"GAP de {abs(gap)} p.p. vs target de {tgt}% "
                f"em {trms} turma(s) com {enc} surveys."
            )

            sent_idx = min(i, len(em_sorted) - 1) if em_sorted else -1
            din_cdu = _EM_DINAMICA.get(
                str(em_sorted[sent_idx]["sentencia"]), din_base
            ) if sent_idx >= 0 else din_base

            top_sent = str(em_sorted[0]["sentencia"]) if em_sorted else ""
            erro_desc = _EM_ERRO.get(
                top_sent,
                f"Comportamento mais critico identificado: {_EM_LABELS.get(top_sent, top_sent)}."
            )

            cdu_items.append({
                "num": i + 1,
                "nome": nome,
                "prioridade": prios[i],
                "pclass": f"p{i+1}",
                "gap": int(round(gap, 0)),
                "turmas": trms,
                "incoming": enc,
                "porque": porque,
                "erro": erro_desc,
                "em_gap": (
                    f"Gap de {_EM_LABELS.get(top_sent, top_sent)}: "
                    "comportamento mais critico identificado nas interacoes do periodo."
                ) if top_sent else "-",
                "dinamica": {
                    "num": i + 1,
                    "titulo": din_cdu.get("titulo", nome),
                    "duracao": din_cdu.get("duracao", "30 min - Pratica"),
                    "objetivo": din_cdu.get("objetivo", f"Melhorar performance no CDU {nome}."),
                    "como": din_cdu.get("como", "-"),
                    "materiais": din_cdu.get("materiais", f"Casos reais do CDU {nome} do periodo."),
                    "tags": din_cdu.get("tags", [])
                }
            })

        max_casos = max((int(r["total_casos"]) for r in team_em), default=1)
        gaps_em = [
            {"label": _EM_LABELS.get(str(r["sentencia"]), str(r["sentencia"])),
             "val": int(r["cnt_bajo"]), "max": max_casos}
            for r in em_sorted if int(r["cnt_bajo"]) > 0
        ]

        if len(gaps_em) >= 2:
            em_insight = (
                f"Os dois comportamentos mais apontados ({gaps_em[0]['label']} e "
                f"{gaps_em[1]['label']}) somam {gaps_em[0]['val'] + gaps_em[1]['val']} registros."
            )
        elif len(gaps_em) == 1:
            em_insight = f"Comportamento mais apontado: {gaps_em[0]['label']} com {gaps_em[0]['val']} registros."
        else:
            em_insight = "Sem dados de Estilo Meli suficientes para o periodo."

        result[team] = {
            "processo": crit_proc,
            "processo_desc": (
                f"Processo com maior impacto negativo para {team}. "
                f"GAP de {abs(crit_gap)} p.p. vs target ({crit_tgt}%) "
                f"com {crit_enc} surveys ({round(crit_pct, 1)}% do incoming total)."
            ),
            "incoming_total": sum(c["incoming"] for c in cdu_items),
            "turmas": max((c.get("turmas", 0) for c in cdu_items), default=0),
            "nps_consolidado": round(crit_nps, 1),
            "gap_total": crit_gap,
            "cdus": cdu_items,
            "gaps_em": gaps_em,
            "em_insight": em_insight,
        }

    return result


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    col_time = datetime.now(tz=timezone(timedelta(hours=-5)))
    log.info("=" * 60)
    log.info(f"KTA Dashboard Refresh — {col_time.strftime('%d/%m/%Y %H:%M')} (Colombia)")
    log.info("=" * 60)

    if not HTML_FILE.exists():
        log.error(f"HTML no encontrado: {HTML_FILE}")
        sys.exit(1)

    log.info("[1/3] BigQuery ...")
    client = get_bq_client()

    nps_monthly   = run_query(client, SQL_NPS_MONTHLY,   "NPS mensual")
    nps_weekly    = run_query(client, SQL_NPS_WEEKLY,    "NPS semanal")
    nps_processos = run_query(client, SQL_NPS_PROCESSOS, "NPS por processo")
    em_data       = run_query(client, SQL_EM,            "Estilo Meli")
    training_data = run_query(client, SQL_TRAINING,      "Training")
    cdu_detail    = run_query(client, SQL_CDU_DETAIL,    "CDU detail")
    em_sentences  = run_query(client, SQL_EM_SENTENCES,  "EM sentencias")
    matriz_faltas = run_query(client, SQL_MATRIZ_FALTAS, "Matriz faltas")

    log.info("[2/3] Actualizando HTML ...")
    html = HTML_FILE.read_text(encoding="utf-8")

    html = inject_or_replace(html, "NPS_MONTHLY",   "monthly",        to_safe_js("monthly",        nps_monthly))
    html = inject_or_replace(html, "NPS_WEEKLY",    "weekly",         to_safe_js("weekly",          nps_weekly))
    html = inject_or_replace(html, "NPS_PROCESSOS", "processos",      to_safe_js("processos",       nps_processos))
    html = inject_or_replace(html, "EM_DATA",       "estiloMeliData", to_safe_js("estiloMeliData",  em_data))

    training_dict = {
        r["USER_TEAM_NAME"]: {
            "pct": r.get("pct"), "hTrain": r.get("hTrain"),
            "hLog": r.get("hLog"), "agentes": r.get("agentes")
        }
        for r in training_data
    }
    html = inject_or_replace(html, "TRAINING_DATA", "trainingData", to_safe_js("trainingData", training_dict))

    cdu_analysis = build_cdu_analysis(nps_processos, cdu_detail, em_sentences, matriz_faltas)
    html = inject_or_replace(html, "CDU_ANALYSIS", "cduAnalysisData", to_safe_js("cduAnalysisData", cdu_analysis))

    # Timestamp
    ts = col_time.strftime("%d/%m/%Y") + " · 06:00h MCO"
    html, _ = re.subn(r'<strong[^>]*id="cxsSync"[^>]*>[^<]*</strong>',
                      f'<strong id="cxsSync">{ts}</strong>', html)
    html, _ = re.subn(r'<span[^>]*id="cxsSync"[^>]*>[^<]*</span>',
                      f'<span id="cxsSync" class="badge-sync-val">{ts}</span>', html)
    html, _ = re.subn(r'<span[^>]*id="headerDate"[^>]*>[^<]*</span>',
                      f'<span class="badge-date" id="headerDate">{ts}</span>', html)

    # Actualizar bloque CSS #kta-ts-css (pseudo-elemento ::after que muestra la fecha)
    def _update_ts_css(m):
        return re.sub(r'(content:\s*")[^"]*(")', lambda m2: m2.group(1) + ts + m2.group(2), m.group(0))
    html = re.sub(r'<style[^>]*id="kta-ts-css"[^>]*>.*?</style>', _update_ts_css, html, flags=re.DOTALL)

    HTML_FILE.write_text(html, encoding="utf-8")
    log.info(f"  HTML guardado: {HTML_FILE.stat().st_size:,} bytes")

    log.info("[3/3] Subiendo a Grid ...")
    grid_token = os.environ.get("GRID_API_TOKEN", "")
    if not grid_token:
        log.warning("  GRID_API_TOKEN no configurado — saltando subida a Grid")
        return

    try:
        import requests
        config = {
            "skill_version": "3.6.0",
            "doc_id": "01KPVNJNMB6CA09M9YZW98TH83",
            "skip_version_check": True,
        }
        with open(HTML_FILE, "rb") as fh:
            resp = requests.post(
                "https://grid.melioffice.com/api/v1/engine/run",
                data={"config": json.dumps(config)},
                files={"file": (HTML_FILE.name, fh, "text/html")},
                headers={"Authorization": f"Bearer {grid_token}"},
                timeout=90,
            )
        data = resp.json()
        steps = data.get("steps", [])
        file_ok = any(
            s.get("label") in ("file_replaced", "uploaded", "version_uploaded")
            and s.get("status") == "OK"
            for s in steps
        )
        if data.get("ok") or file_ok:
            log.info(f"  Grid OK → {data.get('view_url', '')}")
        else:
            log.error(f"  Grid rechazó: {data}")
            if resp.status_code == 401:
                log.error("  Token expirado — renovar GRID_API_TOKEN en GitHub Secrets")
    except Exception as e:
        log.error(f"  Error al subir a Grid: {e}")


if __name__ == "__main__":
    main()
