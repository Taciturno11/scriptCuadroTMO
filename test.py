# etl_cuadro_tmo.py
import requests, pandas as pd, pyodbc, pytz, time
from datetime import datetime, timedelta
import concurrent.futures
import threading

# ───────── CONFIGURACIÓN ────────────────────────────────────────────────
LOGIN_URL    = "http://10.106.17.135:3000/login"
API_URL      = "http://10.106.17.135:3000/api/ds/query"

USUARIO      = "CONRMOLJ"
CLAVE        = "Claro2024"

SQL_SERVER   = "172.16.248.48"
SQL_DATABASE = "Partner"
SQL_USER     = "anubis"
SQL_PASSWORD = "Tg7#kPz9@rLt2025"
SQL_TABLE    = "dbo.Cuadro_TMO"

# Zonas horarias
TZ_LIMA = pytz.timezone("America/Lima")
TZ_UTC  = pytz.utc

# ── CONSTANTES ──────────────────────────────────────────────────────────
ALMOHADA_HORAS = 2
FECHA_INICIO   = datetime(2025, 8, 1, 0, 0, 0, tzinfo=TZ_LIMA)

MAX_REINTENTOS = 3
RETRY_DELAY    = 120

# Ejecutar cada 1 hora
HORARIOS = list(range(24))  # [0, 1, 2, 3, ..., 23]

# Colas a consultar (12 colas)
COLAS = [
    "ACC_InbVent_CrossHogar",
    "ACC_InbVentHogar", 
    "ACC_InbventOC",
    "ACC_Renovinb",
    "CAT_InbCrossHogar",
    "CAT_InbVentHogar",
    "CAT_RenovInb",
    "CCC_InbventOC",
    "PARTNER_InbCrossHogar",
    "PARTNER_InbVentHogar",
    "PARTNER_InbventOC",
    "PARTNER_RenovInb"
]

# Mapeo de nombres de Grafana a SQL Server
COLUMN_MAPPING = {
    'TMO s tHablado/int ': 'TMO s tHablado int',
    'Int. Salientes manuales': 'Int Salientes manuales',
    'Tiempo en int. salientes manuales (H)': 'Tiempo en int salientes manuales (H)',
    'TMO s Int. Salientes manuales': 'TMO s Int Salientes manuales'
}

# Query SQL real de Grafana - Ajustada para nuestro ETL
RAW_SQL_TEMPLATE = """
SELECT
    dIntervalStartUTC as time,
    [cName],
    [cReportGroup],
    SUM(nAbandonedAcd)+SUM(nAnsweredAcd) AS Recibidas,
    SUM(nAnsweredAcd) as Respondidas,
    SUM(nAbandonedAcd) as Abandonadas,
    SUM(nAbandonAcdSvcLvl1) as 'Abandonadas 5s',
    CASE WHEN SUM(nAnsweredAcd)=0 THEN NULL ELSE SUM(tTalkAcd)/sum(nAnsweredAcd) END as 'TMO s tHablado/int ',
    CASE WHEN SUM(tTalkAcd)=0 THEN NULL ELSE ROUND(1.00*SUM(tHoldAcd)/SUM(tTalkAcd),4) END as '% Hold',
    CASE WHEN SUM(nAnsweredAcd)=0 THEN NULL ELSE SUM(tAnsweredAcd)/SUM(nAnsweredAcd) END as 'TME Respondida',
    CASE WHEN SUM(nAbandonedAcd)=0 THEN NULL ELSE SUM(tAbandonedAcd)/SUM(nAbandonedAcd) END as 'TME Abandonada',
    ROUND(1.00*SUM(tAgentAvailable)/3600,2) as 'Tiempo Disponible H',
    ROUND(1.00*(SUM(tAgentOnOtherAcdCall)+SUM(tAgentOnAcdCall))/3600,2) as 'Tiempo Hablado  H',
    ROUND(1.00*SUM(tAgentDnd)/3600,2) as 'Tiempo Recarga  H',
    ROUND(1.00*SUM(tAgentInAcw)/3600,2) as 'Tiempo ACW  H',
    ROUND(1.00*(SUM(tAgentOnNonAcdCall)+SUM(tAgentNotAvailable))/3600,2) as 'Tiempo No Disponible  H',
    ROUND(1.00*SUM(tAgentLoggedIn)/3600,2) as 'Tiempo Total LoggedIn',
    ROUND(1.00*(SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd))/3600,2) as 'Hora ACD',
    CASE WHEN (SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd))=0 THEN NULL ELSE ROUND(1.00*SUM(tAgentAvailable)/(SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd)),4) END as '% Disponible',
    CASE WHEN (SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd))=0 THEN NULL ELSE ROUND(1.00*(SUM(tAgentOnOtherAcdCall)+SUM(tAgentOnAcdCall))/(SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd)),4) END as '% Hablado',
    CASE WHEN (SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd))=0 THEN NULL ELSE ROUND(1.00*SUM(tAgentDnd)/(SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd)),4) END as '% Recarga',
    sum(nInternToExternCalls) - sum(nInternToExternAcdCalls) as 'Int. Salientes manuales',
    ROUND(1.0*(sum(tInternToExternCalls) - sum(tInternToExternAcdCalls))/3600,2) as 'Tiempo en int. salientes manuales (H)',
    CASE WHEN (sum(nInternToExternCalls) - sum(nInternToExternAcdCalls))=0 THEN NULL ELSE (sum(tInternToExternCalls) - sum(tInternToExternAcdCalls))/(sum(nInternToExternCalls) - sum(nInternToExternAcdCalls)) END AS 'TMO s Int. Salientes manuales'
FROM [I3_IC_2020].[dbo].[IAgentQueueStats]
WHERE
    dIntervalStartUTC BETWEEN '{start}' AND '{end}'
    AND cHKey3 ='*'
    AND cHKey4 ='*'
    AND cReportGroup = '{cola}'
    AND cName IN ('ACCBBALC','ACCCSANP','ACCCVARD','ACCGSUAS','ACCJSALL','ACCMAQUIA','ACCPANAD','ACCVALVT','NTGACHA5','ntgasolp','ntgavego','NTGAVIVR','NTGBGAMU','NTGDGUIC','ntgdpuln','ntgdramv','NTGDSUAL','ntgetoas','ntgfsanp','NTGJAGUN','NTGJAMIZ','NTGJCALT','ntgkcaia','NTGKLLAV','NTGLFIET','NTGLRIOA','NTGNALBI','ntgnfigc','NTGVGARL','NTGWJACB','OVGELOPG','OVGLMONB','OVGRMILA','PSGAVILT','PSGCCORM','PSGCRODT','PSGCZARR','PSGEABAP','PSGEGARR','PSGFCCOQ','PSGJMORJ','PSGJSUYS','PSGLCHIQ','PSGLPOMH','PSGLSANG','PSGMVILS','PSGNGERC','PSGSVICC','PSGYMONU')
GROUP BY dIntervalStartUTC, cName, cReportGroup
ORDER BY dIntervalStartUTC, cName ASC
"""

def conectar_sql():
    return pyodbc.connect(
        f"DRIVER={{SQL Server}};SERVER={SQL_SERVER},1433;DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};PWD={SQL_PASSWORD};TrustServerCertificate=yes;"
    )

def ultima_fecha_registrada():
    with conectar_sql() as cnx:
        cursor = cnx.cursor()
        cursor.execute(f"SELECT MAX(time) FROM {SQL_TABLE}")
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
    cols = [f["name"] for f in frame["schema"]["fields"]]
    df = pd.DataFrame(list(zip(*frame["data"]["values"])), columns=cols)
    
    # Mapear nombres de columnas de Grafana a SQL Server
    df.rename(columns=COLUMN_MAPPING, inplace=True)
    
    # Procesar columna time
    if 'time' in df.columns:
        df["time"] = (pd.to_datetime(df["time"].astype(float), unit="ms", utc=True)
                     .dt.tz_convert(TZ_LIMA).dt.tz_localize(None))
    
    return df

def insertar_datos(df, cola):
    if df.empty:
        return 0, 0
    
    with conectar_sql() as cnx:
        cur = cnx.cursor()
        insert = f"""
          INSERT INTO {SQL_TABLE} (
            time, cName, cReportGroup, Recibidas, Respondidas, Abandonadas,
            [Abandonadas 5s], [TMO s tHablado int], [% Hold], [TME Respondida],
            [TME Abandonada], [Tiempo Disponible H], [Tiempo Hablado  H],
            [Tiempo Recarga  H], [Tiempo ACW  H], [Tiempo No Disponible  H],
            [Tiempo Total LoggedIn], [Hora ACD], [% Disponible], [% Hablado],
            [% Recarga], [Int Salientes manuales], 
            [Tiempo en int salientes manuales (H)], [TMO s Int Salientes manuales]
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        nuevos = dup = 0
        for row in df.itertuples(index=False):
            try:
                cur.execute(insert, tuple(row))
                nuevos += 1
            except pyodbc.IntegrityError:
                dup += 1
        cnx.commit()
    return nuevos, dup

def calcular_rango():
    ahora_local = datetime.now(TZ_LIMA)
    ult = ultima_fecha_registrada()
    inicio_local = ult - timedelta(hours=ALMOHADA_HORAS)
    start_utc = inicio_local.astimezone(TZ_UTC).strftime("%Y-%m-%d %H:%M:%S")
    end_utc = ahora_local.astimezone(TZ_UTC).strftime("%Y-%m-%d %H:%M:%S")
    return start_utc, end_utc

def ciclo(cola):
    start, end = calcular_rango()
    print(f"{datetime.now()} – 📥 Consulta: {start} → {end} (UTC) | Cola: {cola}")
    
    try:
        sess = login()
        raw_sql = RAW_SQL_TEMPLATE.format(start=start, end=end, cola=cola)
        df = procesar_datos(consultar_api(sess, start, end, raw_sql))
        nuevos, dup = insertar_datos(df, cola)
        print(f"{datetime.now()} – 📊 [{cola}] Total:{len(df)} | 🆕 {nuevos} | ⚠️ Dup {dup}")
        return True
    except Exception as e:
        print(f"{datetime.now()} – ❌ Error en {cola}: {e}")
        return False

def ciclo_con_reintentos(cola):
    for intento in range(1, MAX_REINTENTOS + 1):
        try:
            if ciclo(cola):
                print(f"{datetime.now()} – ✅ Corte exitoso ({cola}) intento {intento}/{MAX_REINTENTOS}")
                return True
        except Exception as e:
            print(f"{datetime.now()} – ❌ Error en {cola} intento {intento}: {e}")
            if intento < MAX_REINTENTOS:
                print(f"{datetime.now()} – 🔄 Reintentando en {RETRY_DELAY} s…")
                time.sleep(RETRY_DELAY)
            else:
                print(f"{datetime.now()} – 🛑 Corte fallido ({cola}) tras {MAX_REINTENTOS} intentos")
    return False

def procesar_colas_paralelo():
    """Procesar todas las colas en paralelo"""
    print(f"{datetime.now()} – 🚀 Iniciando procesamiento paralelo de {len(COLAS)} colas")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        # Enviar todas las colas al executor
        future_to_cola = {executor.submit(ciclo_con_reintentos, cola): cola for cola in COLAS}
        
        # Recolectar resultados
        resultados = {}
        for future in concurrent.futures.as_completed(future_to_cola):
            cola = future_to_cola[future]
            try:
                resultado = future.result()
                resultados[cola] = resultado
                print(f"{datetime.now()} – ✅ {cola}: {'Exitoso' if resultado else 'Fallido'}")
            except Exception as e:
                print(f"{datetime.now()} – ❌ {cola}: Error - {e}")
                resultados[cola] = False
    
    # Resumen final
    exitosos = sum(1 for r in resultados.values() if r)
    fallidos = len(resultados) - exitosos
    print(f"{datetime.now()} – 📊 RESUMEN: {exitosos} exitosos, {fallidos} fallidos")
    
    return exitosos, fallidos

def proxima_ejecucion():
    while True:
        now = datetime.now(TZ_LIMA)
        candidatos = [
            now.replace(hour=h, minute=0, second=0, microsecond=0) if now.hour < h
            else (now + timedelta(days=1)).replace(hour=h, minute=0, second=0, microsecond=0)
            for h in HORARIOS
        ]
        prox = min(candidatos)
        espera = (prox - now).total_seconds()
        print(f"{datetime.now()} – ⏳ Espera {int(espera)} s → {prox.strftime('%H:%M')}")
        time.sleep(espera)
        yield

if __name__ == "__main__":
    print(f"{datetime.now()} – 🚀 Arranque inmediato ETL Cuadro_TMO")
    print(f"{datetime.now()} – 📊 Procesando {len(COLAS)} colas en paralelo")
    
    # Ejecución inmediata
    exitosos, fallidos = procesar_colas_paralelo()
    
    # Ejecución programada cada hora
    for _ in proxima_ejecucion():
        exitosos, fallidos = procesar_colas_paralelo() 