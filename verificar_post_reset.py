# verificar_post_reset.py
import pyodbc
from datetime import datetime, timedelta
import pytz

# Configuración de conexión
SQL_SERVER   = "172.16.248.48"
SQL_DATABASE = "Partner"
SQL_USER     = "anubis"
SQL_PASSWORD = "Tg7#kPz9@rLt2025"
SQL_TABLE    = "Cuadro_TMO2"

# Zonas horarias
TZ_LIMA = pytz.timezone("America/Lima")

def conectar_sql():
    return pyodbc.connect(
        f"DRIVER={{SQL Server}};SERVER={SQL_SERVER},1433;DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};PWD={SQL_PASSWORD};TrustServerCertificate=yes;"
    )

def verificar_post_reset():
    """Verificar que los datos se cargaron correctamente después del reset"""
    with conectar_sql() as cnx:
        cursor = cnx.cursor()
        
        print("=== VERIFICACIÓN POST RESET COMPLETO ===")
        
        # Fecha de inicio (1 de agosto)
        fecha_inicio = datetime(2025, 8, 1, 0, 0, 0, tzinfo=TZ_LIMA)
        
        # 1. Verificar total de registros
        cursor.execute(f"SELECT COUNT(*) FROM {SQL_TABLE}")
        total_registros = cursor.fetchone()[0]
        print(f"📊 TOTAL DE REGISTROS: {total_registros:,}")
        
        # 2. Verificar rango de fechas
        cursor.execute(f"""
            SELECT 
                MIN(time) as fecha_minima,
                MAX(time) as fecha_maxima
            FROM {SQL_TABLE}
        """)
        fecha_min, fecha_max = cursor.fetchone()
        print(f"📅 RANGO DE FECHAS: {fecha_min} a {fecha_max}")
        
        # 3. Verificar datos por cola
        cursor.execute(f"""
            SELECT 
                cReportGroup,
                COUNT(*) as registros,
                COUNT(DISTINCT cName) as agentes_unicos
            FROM {SQL_TABLE}
            GROUP BY cReportGroup
            ORDER BY cReportGroup
        """)
        
        colas = cursor.fetchall()
        print(f"\n📈 DATOS POR COLA:")
        for cola, registros, agentes in colas:
            print(f"   {cola}: {registros:,} registros | {agentes} agentes únicos")
        
        # 4. Verificar agentes específicos
        cursor.execute(f"""
            SELECT 
                cName,
                COUNT(*) as registros_por_agente
            FROM {SQL_TABLE}
            GROUP BY cName
            ORDER BY registros_por_agente DESC
        """)
        
        agentes = cursor.fetchall()
        print(f"\n👥 TOP 10 AGENTES CON MÁS DATOS:")
        for agente, registros in agentes[:10]:
            print(f"   {agente}: {registros:,} registros")
        
        # 5. Verificar PSGGPERL específicamente
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM {SQL_TABLE} 
            WHERE cName = 'PSGGPERL'
        """)
        psggperl_count = cursor.fetchone()[0]
        
        print(f"\n🔍 VERIFICACIÓN PSGGPERL:")
        print(f"   PSGGPERL registros: {psggperl_count:,}")
        
        if psggperl_count == 0:
            print(f"   ⚠️  PSGGPERL NO tiene datos - PROBLEMA DETECTADO")
        else:
            print(f"   ✅ PSGGPERL SÍ tiene datos - TODO CORRECTO")
        
        # 6. Verificar distribución por horas
        cursor.execute(f"""
            SELECT 
                DATEADD(HOUR, DATEDIFF(HOUR, 0, time), 0) as hora,
                COUNT(*) as registros_por_hora
            FROM {SQL_TABLE}
            GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, time), 0)
            ORDER BY hora DESC
        """)
        
        horas = cursor.fetchall()
        print(f"\n🕐 ÚLTIMAS 10 HORAS CON DATOS:")
        for hora, registros_hora in horas[:10]:
            print(f"   {hora}: {registros_hora:,} registros")
        
        # 7. Verificar si hay duplicados
        cursor.execute(f"""
            SELECT 
                COUNT(*) - COUNT(DISTINCT CONCAT(CAST(time AS VARCHAR(50)), '|', ISNULL(cName, ''), '|', ISNULL(cReportGroup, ''))) as duplicados
            FROM {SQL_TABLE}
        """)
        
        duplicados = cursor.fetchone()[0]
        print(f"\n🔍 VERIFICACIÓN DE DUPLICADOS:")
        print(f"   Duplicados encontrados: {duplicados:,}")
        
        if duplicados == 0:
            print(f"   ✅ NO hay duplicados - PERFECTO")
        else:
            print(f"   ⚠️  HAY duplicados - REVISAR")
        
        # 8. Resumen final
        print(f"\n{'='*60}")
        print(f"RESUMEN POST RESET")
        print(f"{'='*60}")
        print(f"✅ Total registros: {total_registros:,}")
        print(f"✅ Rango de fechas: {fecha_min} a {fecha_max}")
        print(f"✅ Colas procesadas: {len(colas)}")
        print(f"✅ Agentes únicos: {len(agentes)}")
        print(f"✅ PSGGPERL incluido: {'SÍ' if psggperl_count > 0 else 'NO'}")
        print(f"✅ Duplicados: {'NO' if duplicados == 0 else 'SÍ'}")
        
        # 9. Recomendaciones
        print(f"\n🎯 RECOMENDACIONES:")
        if psggperl_count == 0:
            print(f"   ⚠️  PSGGPERL no tiene datos - Revisar script de carga")
        if duplicados > 0:
            print(f"   ⚠️  Hay duplicados - Considerar limpieza")
        if total_registros == 0:
            print(f"   ❌ No hay datos - Revisar proceso de carga")
        else:
            print(f"   ✅ Reset completado exitosamente")
        
        return total_registros, psggperl_count, duplicados

if __name__ == "__main__":
    verificar_post_reset() 