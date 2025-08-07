# etl_unificada.py
import requests, pandas as pd, pyodbc, pytz, time
from datetime import datetime, timedelta

# ───────── CONFIGURACIÓN ────────────────────────────────────────────────
LOGIN_URL    = "http://10.106.17.135:3000/login"
API_URL      = "http://10.106.17.135:3000/api/ds/query"

USUARIO      = "CONRMOLJ"
CLAVE        = "Claro2024"

SQL_SERVER   = "172.16.248.48"
SQL_DATABASE = "Partner"
SQL_USER     = "anubis"
SQL_PASSWORD = "Tg7#kPz9@rLt2025"
SQL_TABLE    = "dbo.CategoriaAAA_POST"  # Misma tabla para ambas colas

# Zonas horarias
TZ_LIMA = pytz.timezone("America/Lima")
TZ_UTC  = pytz.utc

# ── CONSTANTES ──────────────────────────────────────────────────────────
ALMOHADA_HORAS = 2
FECHA_INICIO   = datetime(2025, 6, 1, 0, 0, 0, tzinfo=TZ_LIMA)

MAX_REINTENTOS = 3
RETRY_DELAY    = 120

HORARIOS = [0, 2, 4, 6, 10, 12, 14, 15, 16, 17, 18, 19, 22]

# Colas a consultar
COLAS = [
    "CategoriaAAA_POST",
    "CategoriaAAA_Formadepago_POST"
]

# Plantilla SQL con placeholder {cola}
RAW_SQL_TEMPLATE = """
SELECT
  CONCAT('IDK ', IW.InteractionIDKey) as InteractionIDKey,
  I.RemoteID                as Numero,
  I.RemoteName              as Nombre,
  ICA.CustomString4         as Cedula,
  IW.WrapupStartDateTimeUTC as Time,
  IW.UserID                 as Usuario,
  IW.WorkgroupID            as Cola,
  IW.WrapupCategory         as Categoria,
  IW.WrapupCode             as Codigo,
  ICA.CustomString3         as PerfilTransfer,
  LastAssignedWorkgroupID   as UltimaCola,
  i.DNIS_LocalID
FROM   [I3_IC_2020].[dbo].[InteractionWrapup] IW
LEFT JOIN [I3_IC_2020].[dbo].[InteractionSummary]          I  ON IW.InteractionIDKey = I.InteractionIDKey
LEFT JOIN [I3_IC_2020].[dbo].[InteractionCustomAttributes] ICA ON IW.InteractionIDKey = ICA.InteractionIDKey
WHERE  IW.WrapupStartDateTimeUTC BETWEEN '{start}' AND '{end}'
  AND  IW.WorkgroupID = '{cola}'
"""

def conectar_sql():
    return pyodbc.connect(
        f"DRIVER={{SQL Server}};SERVER={SQL_SERVER},1433;DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};PWD={SQL_PASSWORD};TrustServerCertificate=yes;"
    )

def ultima_fecha_registrada():
    with conectar_sql() as cnx:
        cursor = cnx.cursor()
        cursor.execute(f"SELECT MAX(Time) FROM {SQL_TABLE}")
        ts = cursor.fetchone()[0]
    if ts is None:
        return FECHA_INICIO
    if ts.tzinfo is None:
        ts = TZ_LIMA.localize(ts)
    else:
        ts = ts.astimezone(TZ_LIMA)
    return ts

def login():
    s = requests.Session()
    s.post(LOGIN_URL, json={"user": USUARIO, "password": CLAVE},
           headers={"Content-Type": "application/json"}).raise_for_status()
    print(f"{datetime.now()} – ✅ Login exitoso")
    return s

def consultar_api(sess, start, end, raw_sql):
    payload = {
        "queries": [{
            "refId": "A",
            "datasource": {"type": "mssql", "uid": "PZT_sj4Gz"},
            "rawSql": raw_sql,
            "format": "table"
        }],
        "range": {"from": start, "to": end}
    }
    r = sess.post(API_URL, json=payload); r.raise_for_status()
    return r.json()

def procesar_datos(j):
    frame = j["results"]["A"]["frames"][0]
    cols  = [f["name"] for f in frame["schema"]["fields"]]
    df    = pd.DataFrame(list(zip(*frame["data"]["values"])), columns=cols)
    df.rename(columns={"Perfil Trasnfer.": "PerfilTransfer"}, inplace=True)
    df["Time"] = (pd.to_datetime(df["Time"].astype(float), unit="ms", utc=True)
                     .dt.tz_convert(TZ_LIMA).dt.tz_localize(None))
    return df

def insertar_datos(df):
    if df.empty:
        return 0, 0
    with conectar_sql() as cnx:
        cur = cnx.cursor()
        insert = f"""
          INSERT INTO {SQL_TABLE} (
            ID, Numero, Nombre, Cedula, Time, Usuario,
            Cola, Categoria, Codigo, PerfilTransfer, UltimaCola, DNIS_LocalID, FechaCarga
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
        """
        nuevos = dup = 0
        for row in df.itertuples(index=False):
            try:
                cur.execute(insert,
                            row.InteractionIDKey, row.Numero, row.Nombre,
                            row.Cedula, row.Time, row.Usuario, row.Cola,
                            row.Categoria, row.Codigo, row.PerfilTransfer,
                            row.UltimaCola, row.DNIS_LocalID)
                nuevos += 1
            except pyodbc.IntegrityError:
                dup += 1
        cnx.commit()
    return nuevos, dup

def calcular_rango():
    ahora_local  = datetime.now(TZ_LIMA)
    ult          = ultima_fecha_registrada()
    inicio_local = ult - timedelta(hours=ALMOHADA_HORAS)
    start_utc    = inicio_local.astimezone(TZ_UTC).strftime("%Y-%m-%d %H:%M:%S")
    end_utc      = ahora_local.astimezone(TZ_UTC).strftime("%Y-%m-%d %H:%M:%S")
    return start_utc, end_utc

def ciclo(cola):
    start, end = calcular_rango()
    print(f"{datetime.now()} – 📥 Consulta: {start} → {end} (UTC) | Cola: {cola}")
    sess    = login()
    raw_sql = RAW_SQL_TEMPLATE.format(start=start, end=end, cola=cola)
    df      = procesar_datos(consultar_api(sess, start, end, raw_sql))
    nuevos, dup = insertar_datos(df)
    print(f"{datetime.now()} – 📊 [{cola}] Total:{len(df)} | 🆕 {nuevos} | ⚠️ Dup {dup}")

def ciclo_con_reintentos(cola):
    for intento in range(1, MAX_REINTENTOS + 1):
        try:
            ciclo(cola)
            print(f"{datetime.now()} – ✅ Corte exitoso ({cola}) intento {intento}/{MAX_REINTENTOS}")
            return
        except Exception as e:
            print(f"{datetime.now()} – ❌ Error en {cola} intento {intento}: {e}")
            if intento < MAX_REINTENTOS:
                print(f"{datetime.now()} – 🔄 Reintentando en {RETRY_DELAY} s…")
                time.sleep(RETRY_DELAY)
            else:
                print(f"{datetime.now()} – 🛑 Corte fallido ({cola}) tras {MAX_REINTENTOS} intentos")

def proxima_ejecucion():
    while True:
        now = datetime.now(TZ_LIMA)
        candidatos = [
            now.replace(hour=h, minute=0, second=0, microsecond=0) if now.hour < h
            else (now + timedelta(days=1)).replace(hour=h, minute=0, second=0, microsecond=0)
            for h in HORARIOS
        ]
        prox   = min(candidatos)
        espera = (prox - now).total_seconds()
        print(f"{datetime.now()} – ⏳ Espera {int(espera)} s → {prox.strftime('%H:%M')}")
        time.sleep(espera)
        yield

if __name__ == "__main__":
    print(f"{datetime.now()} – 🚀 Arranque inmediato")
    for cola in COLAS:
        ciclo_con_reintentos(cola)
    for _ in proxima_ejecucion():
        for cola in COLAS:
            ciclo_con_reintentos(cola)
