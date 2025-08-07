# verificar_datos_perdidos.py
import pyodbc
from datetime import datetime, timedelta
import pytz

# Configuración de conexión
SQL_SERVER   = "172.16.248.48"
SQL_DATABASE = "Partner"
SQL_USER     = "anubis"
SQL_PASSWORD = "Tg7#kPz9@rLt2025"

# Zonas horarias
TZ_LIMA = pytz.timezone("America/Lima")

def conectar_sql():
    return pyodbc.connect(
        f"DRIVER={{SQL Server}};SERVER={SQL_SERVER},1433;DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};PWD={SQL_PASSWORD};TrustServerCertificate=yes;"
    )

def verificar_datos_perdidos():
    """Verificar qué datos se perdieron desde el 1 de agosto"""
    with conectar_sql() as cnx:
        cursor = cnx.cursor()
        
        print("=== VERIFICACIÓN DE DATOS PERDIDOS DESDE 1 DE AGOSTO ===")
        
        # Fecha de inicio (1 de agosto)
        fecha_inicio = datetime(2025, 8, 1, 0, 0, 0, tzinfo=TZ_LIMA)
        
        # Verificar datos actuales en Cuadro_TMO2
        cursor.execute("""
            SELECT 
                COUNT(*) as total_registros,
                MIN(time) as fecha_minima,
                MAX(time) as fecha_maxima
            FROM [dbo].[Cuadro_TMO2]
            WHERE time >= ?
        """, (fecha_inicio,))
        
        resultado = cursor.fetchone()
        total_actual = resultado[0]
        fecha_min = resultado[1]
        fecha_max = resultado[2]
        
        print(f"📊 DATOS ACTUALES EN Cuadro_TMO2:")
        print(f"   Total registros desde 1 de agosto: {total_actual:,}")
        print(f"   Fecha mínima: {fecha_min}")
        print(f"   Fecha máxima: {fecha_max}")
        
        # Verificar datos por cola
        cursor.execute("""
            SELECT 
                cReportGroup,
                COUNT(*) as registros,
                MIN(time) as fecha_minima,
                MAX(time) as fecha_maxima
            FROM [dbo].[Cuadro_TMO2]
            WHERE time >= ?
            GROUP BY cReportGroup
            ORDER BY cReportGroup
        """, (fecha_inicio,))
        
        colas = cursor.fetchall()
        
        print(f"\n📈 DATOS POR COLA:")
        for cola, registros, fecha_min_cola, fecha_max_cola in colas:
            print(f"   {cola}: {registros:,} registros ({fecha_min_cola} a {fecha_max_cola})")
        
        # Verificar si hay datos faltantes por hora
        cursor.execute("""
            SELECT 
                DATEADD(HOUR, DATEDIFF(HOUR, 0, time), 0) as hora,
                COUNT(*) as registros_por_hora
            FROM [dbo].[Cuadro_TMO2]
            WHERE time >= ?
            GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, time), 0)
            ORDER BY hora
        """, (fecha_inicio,))
        
        horas = cursor.fetchall()
        
        print(f"\n🕐 DISTRIBUCIÓN POR HORAS:")
        for hora, registros_hora in horas[:10]:  # Mostrar solo las primeras 10 horas
            print(f"   {hora}: {registros_hora:,} registros")
        
        if len(horas) > 10:
            print(f"   ... y {len(horas) - 10} horas más")
        
        # Verificar agentes faltantes
        cursor.execute("""
            SELECT 
                cName,
                COUNT(*) as registros_por_agente
            FROM [dbo].[Cuadro_TMO2]
            WHERE time >= ?
            GROUP BY cName
            ORDER BY registros_por_agente DESC
        """, (fecha_inicio,))
        
        agentes = cursor.fetchall()
        
        print(f"\n👥 AGENTES CON DATOS:")
        for agente, registros_agente in agentes[:10]:  # Mostrar solo los primeros 10 agentes
            print(f"   {agente}: {registros_agente:,} registros")
        
        if len(agentes) > 10:
            print(f"   ... y {len(agentes) - 10} agentes más")
        
        # Verificar si PSGGPERL está presente
        cursor.execute("""
            SELECT COUNT(*) 
            FROM [dbo].[Cuadro_TMO2] 
            WHERE cName = 'PSGGPERL' AND time >= ?
        """, (fecha_inicio,))
        
        psggperl_count = cursor.fetchone()[0]
        
        print(f"\n🔍 VERIFICACIÓN ESPECÍFICA:")
        print(f"   PSGGPERL registros: {psggperl_count:,}")
        
        if psggperl_count == 0:
            print(f"   ⚠️  PSGGPERL NO tiene datos desde el 1 de agosto")
        else:
            print(f"   ✅ PSGGPERL SÍ tiene datos desde el 1 de agosto")
        
        return total_actual, fecha_min, fecha_max

if __name__ == "__main__":
    verificar_datos_perdidos() 