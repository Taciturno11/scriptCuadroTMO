# reset_completo_desde_agosto.py
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
SQL_TABLE    = "Cuadro_TMO2"

# Zonas horarias
TZ_LIMA = pytz.timezone("America/Lima")
TZ_UTC  = pytz.utc

# ── CONSTANTES ──────────────────────────────────────────────────────────
FECHA_INICIO   = datetime(2025, 8, 1, 0, 0, 0, tzinfo=TZ_LIMA)  # Exactamente 00:00:00 del 1 de agosto

# Colas a consultar
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

# Plantilla SQL con placeholder {cola}
RAW_SQL_TEMPLATE = """
SELECT
    DATEADD(HOUR, DATEDIFF(HOUR, 0, dIntervalStartUTC), 0) as time,
    [cName],
    [cReportGroup],
    SUM(nAbandonedAcd)+SUM(nAnsweredAcd) AS Recibidas,
    SUM(nAnsweredAcd) as Respondidas,
    SUM(nAbandonedAcd) as Abandonadas,
    SUM(nAbandonAcdSvcLvl1) as 'Abandonadas 5s',
    CASE WHEN SUM(nAnsweredAcd)=0 THEN NULL ELSE SUM(tTalkAcd)/sum(nAnsweredAcd) END as 'TMO s tHablado/int ',
    CASE WHEN SUM(tTalkAcd)=0 THEN NULL ELSE ROUND(1.00*SUM(tHoldAcd)/SUM(tTalkAcd)*100,2) END as '% Hold',
    CASE WHEN SUM(nAnsweredAcd)=0 THEN NULL ELSE SUM(tAnsweredAcd)/SUM(nAnsweredAcd) END as 'TME Respondida',
    CASE WHEN SUM(nAbandonedAcd)=0 THEN NULL ELSE SUM(tAbandonedAcd)/SUM(nAbandonedAcd) END as 'TME Abandonada',
    ROUND(1.00*SUM(tAgentAvailable)/3600,2) as 'Tiempo Disponible H',
    ROUND(1.00*(SUM(tAgentOnOtherAcdCall)+SUM(tAgentOnAcdCall))/3600,2) as 'Tiempo Hablado  H',
    ROUND(1.00*SUM(tAgentDnd)/3600,2) as 'Tiempo Recarga  H',
    ROUND(1.00*SUM(tAgentInAcw)/3600,2) as 'Tiempo ACW  H',
    ROUND(1.00*(SUM(tAgentOnNonAcdCall)+SUM(tAgentNotAvailable))/3600,2) as 'Tiempo No Disponible  H',
    ROUND(1.00*SUM(tAgentLoggedIn)/3600,2) as 'Tiempo Total LoggedIn',
    ROUND(1.00*(SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd))/3600,2) as 'Hora ACD',
    CASE WHEN (SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd))=0 THEN NULL ELSE ROUND(1.00*SUM(tAgentAvailable)/(SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd))*100,2) END as '% Disponible',
    CASE WHEN (SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd))=0 THEN NULL ELSE ROUND(1.00*(SUM(tAgentOnOtherAcdCall)+SUM(tAgentOnAcdCall))/(SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd))*100,2) END as '% Hablado',
    CASE WHEN (SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd))=0 THEN NULL ELSE ROUND(1.00*SUM(tAgentDnd)/(SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd))*100,2) END as '% Recarga',
    sum(nInternToExternCalls) - sum(nInternToExternAcdCalls) as 'Int. Salientes manuales',
    ROUND(1.0*(sum(tInternToExternCalls) - sum(tInternToExternAcdCalls))/3600,2) as 'Tiempo en int. salientes manuales (H)',
    CASE WHEN (sum(nInternToExternCalls) - sum(nInternToExternAcdCalls))=0 THEN NULL ELSE (sum(tInternToExternCalls) - sum(tInternToExternAcdCalls))/(sum(nInternToExternCalls) - sum(nInternToExternAcdCalls)) END AS 'TMO s Int. Salientes manuales'
FROM [I3_IC_2020].[dbo].[IAgentQueueStats]
WHERE
    dIntervalStartUTC BETWEEN '{start}' AND '{end}'
    AND cHKey3 ='*'
    AND cHKey4 ='*'
    AND cReportGroup = '{cola}'
    AND cName IN ('ACCBBALC','ACCCSANP','ACCCVARD','ACCGSUAS','ACCJSALL','ACCMAQUIA','ACCPANAD','ACCVALVT','NTGACHA5','ntgasolp','ntgavego','NTGAVIVR','NTGBGAMU','NTGDGUIC','ntgdpuln','ntgdramv','NTGDSUAL','ntgetoas','ntgfsanp','NTGJAGUN','NTGJAMIZ','NTGJCALT','ntgkcaia','NTGKLLAV','NTGLFIET','NTGLRIOA','NTGNALBI','ntgnfigc','NTGVGARL','NTGWJACB','OVGELOPG','OVGLMONB','OVGRMILA','PSGAVILT','PSGCCORM','PSGCRODT','PSGCZARR','PSGEABAP','PSGEGARR','PSGFCCOQ','PSGJMORJ','PSGJSUYS','PSGLCHIQ','PSGLPOMH','PSGLSANG','PSGMVILS','PSGNGERC','PSGSVICC','PSGYMONU','PSGGPERL')
GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, dIntervalStartUTC), 0), cName, cReportGroup
ORDER BY DATEADD(HOUR, DATEDIFF(HOUR, 0, dIntervalStartUTC), 0), cName ASC
"""

def conectar_sql():
    return pyodbc.connect(
        f"DRIVER={{SQL Server}};SERVER={SQL_SERVER},1433;DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};PWD={SQL_PASSWORD};TrustServerCertificate=yes;"
    )

def borrar_todos_los_datos():
    """Borra todos los datos de la tabla Cuadro_TMO2"""
    with conectar_sql() as cnx:
        cursor = cnx.cursor()
        
        print(f"{datetime.now()} – 🗑️  BORRANDO TODOS LOS DATOS DE {SQL_TABLE}")
        
        # Contar registros antes de borrar
        cursor.execute(f"SELECT COUNT(*) FROM {SQL_TABLE}")
        registros_antes = cursor.fetchone()[0]
        print(f"{datetime.now()} – 📊 Registros antes de borrar: {registros_antes:,}")
        
        # Borrar todos los datos
        cursor.execute(f"DELETE FROM {SQL_TABLE}")
        registros_borrados = cursor.rowcount
        cnx.commit()
        
        print(f"{datetime.now()} – ✅ DATOS BORRADOS: {registros_borrados:,} registros")
        
        # Verificar que la tabla esté vacía
        cursor.execute(f"SELECT COUNT(*) FROM {SQL_TABLE}")
        registros_despues = cursor.fetchone()[0]
        print(f"{datetime.now()} – 📊 Registros después de borrar: {registros_despues:,}")
        
        if registros_despues == 0:
            print(f"{datetime.now()} – ✅ Tabla completamente vacía")
        else:
            print(f"{datetime.now()} – ⚠️  Tabla NO está completamente vacía")
        
        return registros_borrados

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
    df["time"] = (pd.to_datetime(df["time"].astype(float), unit="ms", utc=True)
                     .dt.tz_convert(TZ_LIMA).dt.tz_localize(None))
    
    # Limpiar valores nulos - convertir strings vacíos y 'NULL' a None
    for col in df.columns:
        if col != 'time':  # No tocar la columna time
            df[col] = df[col].replace(['', 'NULL', 'null'], None)
    
    return df

def insertar_datos(df):
    if df.empty:
        return 0
    with conectar_sql() as cnx:
        cur = cnx.cursor()
        
        insert = f"""
          INSERT INTO {SQL_TABLE} (
            time, cName, cReportGroup, Recibidas, Respondidas, Abandonadas, 
            [Abandonadas 5s], [TMO s tHablado/int ], [% Hold], [TME Respondida], 
            [TME Abandonada], [Tiempo Disponible H], [Tiempo Hablado  H], 
            [Tiempo Recarga  H], [Tiempo ACW  H], [Tiempo No Disponible  H], 
            [Tiempo Total LoggedIn], [Hora ACD], [% Disponible], [% Hablado], 
            [% Recarga], [Int. Salientes manuales], [Tiempo en int. salientes manuales (H)], 
            [TMO s Int. Salientes manuales], fechaCarga
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
        """
        insertados = 0
        for row in df.itertuples(index=False):
            try:
                # Convertir valores vacíos o nulos a None
                valores = []
                for value in row:
                    if pd.isna(value) or value == '' or value == 'NULL' or value == 'null' or str(value).strip() == '':
                        valores.append(None)
                    else:
                        valores.append(value)
                
                # Insertar registro
                cur.execute(insert, valores)
                insertados += 1
                    
            except Exception as e:
                print(f"{datetime.now()} – ⚠️ Error insertando fila: {e}")
        
        cnx.commit()
    return insertados

def procesar_cola_desde_agosto(cola):
    """Procesa una cola específica desde el 1 de agosto hasta ahora"""
    ahora_local = datetime.now(TZ_LIMA)
    start_utc = FECHA_INICIO.astimezone(TZ_UTC).strftime("%Y-%m-%d %H:%M:%S")
    end_utc = ahora_local.astimezone(TZ_UTC).strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"{datetime.now()} – 📥 Procesando {cola}: {start_utc} → {end_utc} (UTC)")
    
    sess = login()
    raw_sql = RAW_SQL_TEMPLATE.format(start=start_utc, end=end_utc, cola=cola)
    df = procesar_datos(consultar_api(sess, start_utc, end_utc, raw_sql))
    
    if not df.empty:
        agentes_en_df = df['cName'].unique().tolist()
        print(f"{datetime.now()} – 🔍 [{cola}]: {len(agentes_en_df)} agentes encontrados: {agentes_en_df[:5]}{'...' if len(agentes_en_df) > 5 else ''}")
        
        # Verificar si PSGGPERL está en la lista
        if 'PSGGPERL' in agentes_en_df:
            print(f"{datetime.now()} – ✅ PSGGPERL encontrado en {cola}")
        else:
            print(f"{datetime.now()} – ⚠️ PSGGPERL NO encontrado en {cola}")
    
    insertados = insertar_datos(df)
    print(f"{datetime.now()} – 📊 [{cola}] Total:{len(df)} | 🆕 Insertados: {insertados}")
    
    return len(df), insertados

def main():
    print(f"{datetime.now()} – 🚀 INICIANDO RESET COMPLETO DESDE 1 DE AGOSTO")
    print(f"{datetime.now()} – 📅 Fecha de inicio: {FECHA_INICIO.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{datetime.now()} – 🎯 Objetivo: Borrar todos los datos y recargar desde el 1 de agosto")
    
    # Confirmar con el usuario
    print(f"\n⚠️  ADVERTENCIA: Esto borrará TODOS los datos de {SQL_TABLE}")
    respuesta = input("¿Estás seguro de que quieres continuar? (s/n): ").lower()
    
    if respuesta not in ['s', 'si', 'sí', 'y', 'yes']:
        print(f"{datetime.now()} – ❌ Operación cancelada por el usuario")
        return
    
    try:
        # PASO 1: Borrar todos los datos
        print(f"\n{'='*60}")
        print(f"PASO 1: BORRANDO TODOS LOS DATOS")
        print(f"{'='*60}")
        registros_borrados = borrar_todos_los_datos()
        
        # PASO 2: Recargar datos desde el 1 de agosto
        print(f"\n{'='*60}")
        print(f"PASO 2: RECARGANDO DATOS DESDE 1 DE AGOSTO")
        print(f"{'='*60}")
        
        total_registros = 0
        total_insertados = 0
        
        for cola in COLAS:
            try:
                registros, insertados = procesar_cola_desde_agosto(cola)
                total_registros += registros
                total_insertados += insertados
                print(f"{datetime.now()} – ✅ Completado: {cola}")
            except Exception as e:
                print(f"{datetime.now()} – ❌ Error en {cola}: {e}")
        
        # RESUMEN FINAL
        print(f"\n{'='*60}")
        print(f"RESUMEN FINAL")
        print(f"{'='*60}")
        print(f"📊 Registros borrados: {registros_borrados:,}")
        print(f"📊 Registros procesados: {total_registros:,}")
        print(f"📊 Registros insertados: {total_insertados:,}")
        print(f"📊 Registros perdidos: {total_registros - total_insertados:,}")
        print(f"🎯 Reset completo finalizado")
        
    except Exception as e:
        print(f"{datetime.now()} – ❌ Error durante el reset: {e}")

if __name__ == "__main__":
    main() 