# limpiar_duplicados.py
import pyodbc
from datetime import datetime

# Configuración de conexión
SQL_SERVER   = "172.16.248.48"
SQL_DATABASE = "Partner"
SQL_USER     = "anubis"
SQL_PASSWORD = "Tg7#kPz9@rLt2025"

def conectar_sql():
    return pyodbc.connect(
        f"DRIVER={{SQL Server}};SERVER={SQL_SERVER},1433;DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};PWD={SQL_PASSWORD};TrustServerCertificate=yes;"
    )

def analizar_duplicados():
    """Analizar cuántos duplicados hay en la tabla"""
    with conectar_sql() as cnx:
        cursor = cnx.cursor()
        
        print("=== ANÁLISIS DE DUPLICADOS EN Cuadro_TMO ===")
        
        # Contar total de registros
        cursor.execute(f"SELECT COUNT(*) FROM [dbo].[Cuadro_TMO]")
        total_registros = cursor.fetchone()[0]
        print(f"📊 TOTAL DE REGISTROS: {total_registros:,}")
        
        # Identificar duplicados
        cursor.execute("""
            SELECT 
                time, cName, cReportGroup, COUNT(*) as cantidad
            FROM [dbo].[Cuadro_TMO]
            GROUP BY time, cName, cReportGroup
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
        """)
        
        duplicados = cursor.fetchall()
        
        if duplicados:
            print(f"\n⚠️  DUPLICADOS ENCONTRADOS: {len(duplicados)} combinaciones únicas")
            
            total_duplicados = sum(row[3] for row in duplicados)
            registros_a_eliminar = total_duplicados - len(duplicados)
            
            print(f"📈 TOTAL REGISTROS DUPLICADOS: {total_duplicados:,}")
            print(f"🗑️  REGISTROS A ELIMINAR: {registros_a_eliminar:,}")
            print(f"✅ REGISTROS A MANTENER: {total_registros - registros_a_eliminar:,}")
            
            # Mostrar algunos ejemplos
            print(f"\n🔍 EJEMPLOS DE DUPLICADOS:")
            for i, (time, cName, cReportGroup, cantidad) in enumerate(duplicados[:5]):
                print(f"  {i+1}. {time} | {cName} | {cReportGroup} | {cantidad} registros")
            
            if len(duplicados) > 5:
                print(f"  ... y {len(duplicados) - 5} combinaciones más")
            
            return True, total_duplicados, registros_a_eliminar
        else:
            print("✅ NO HAY DUPLICADOS ENCONTRADOS")
            return False, 0, 0

def limpiar_duplicados():
    """Eliminar duplicados manteniendo solo el registro más reciente (por fechaCarga)"""
    with conectar_sql() as cnx:
        cursor = cnx.cursor()
        
        print("\n=== LIMPIANDO DUPLICADOS ===")
        
        # Primero, contar cuántos registros vamos a eliminar
        cursor.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT CONCAT(CAST(time AS VARCHAR(50)), '|', cName, '|', cReportGroup))
            FROM [dbo].[Cuadro_TMO]
        """)
        registros_a_eliminar = cursor.fetchone()[0]
        
        if registros_a_eliminar == 0:
            print("✅ No hay duplicados para eliminar")
            return 0
        
        print(f"🗑️  Eliminando {registros_a_eliminar:,} registros duplicados...")
        
        # Crear tabla temporal con los registros únicos (manteniendo el más reciente)
        cursor.execute("""
            SELECT time, cName, cReportGroup, MAX(fechaCarga) as max_fechaCarga
            INTO #temp_unicos
            FROM [dbo].[Cuadro_TMO]
            GROUP BY time, cName, cReportGroup
        """)
        
        # Eliminar registros que no están en la tabla temporal
        cursor.execute("""
            DELETE t1
            FROM [dbo].[Cuadro_TMO] t1
            LEFT JOIN #temp_unicos t2 
                ON t1.time = t2.time 
                AND t1.cName = t2.cName 
                AND t1.cReportGroup = t2.cReportGroup
                AND t1.fechaCarga = t2.max_fechaCarga
            WHERE t2.time IS NULL
        """)
        
        registros_eliminados = cursor.rowcount
        cnx.commit()
        
        print(f"✅ DUPLICADOS ELIMINADOS: {registros_eliminados:,} registros")
        
        # Verificar resultado
        cursor.execute(f"SELECT COUNT(*) FROM [dbo].[Cuadro_TMO]")
        registros_finales = cursor.fetchone()[0]
        print(f"📊 REGISTROS FINALES: {registros_finales:,}")
        
        return registros_eliminados

def main():
    print(f"{datetime.now()} – 🚀 INICIANDO LIMPIEZA DE DUPLICADOS")
    
    try:
        # Analizar duplicados
        hay_duplicados, total_duplicados, a_eliminar = analizar_duplicados()
        
        if hay_duplicados:
            print(f"\n¿Deseas eliminar {a_eliminar:,} registros duplicados? (s/n): ", end="")
            respuesta = input().lower()
            
            if respuesta in ['s', 'si', 'sí', 'y', 'yes']:
                eliminados = limpiar_duplicados()
                print(f"\n🎉 LIMPIEZA COMPLETADA: {eliminados:,} registros eliminados")
            else:
                print("❌ Limpieza cancelada por el usuario")
        else:
            print("✅ No es necesario limpiar duplicados")
            
    except Exception as e:
        print(f"❌ Error durante la limpieza: {e}")

if __name__ == "__main__":
    main() 