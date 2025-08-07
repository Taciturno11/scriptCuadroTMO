# limpiar_duplicados_cuadro_tmo2.py
import pyodbc
from datetime import datetime

# Configuración de conexión
SQL_SERVER   = "172.16.248.48"
SQL_DATABASE = "Partner"
SQL_USER     = "anubis"
SQL_PASSWORD = "Tg7#kPz9@rLt2025"
SQL_TABLE    = "Cuadro_TMO2"

def conectar_sql():
    return pyodbc.connect(
        f"DRIVER={{SQL Server}};SERVER={SQL_SERVER},1433;DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};PWD={SQL_PASSWORD};TrustServerCertificate=yes;"
    )

def analizar_duplicados():
    """Analizar cuántos duplicados hay en la tabla Cuadro_TMO2"""
    with conectar_sql() as cnx:
        cursor = cnx.cursor()
        
        print("=== ANÁLISIS DE DUPLICADOS EN Cuadro_TMO2 ===")
        
        # Contar total de registros
        cursor.execute(f"SELECT COUNT(*) FROM [dbo].[{SQL_TABLE}]")
        total_registros = cursor.fetchone()[0]
        print(f"📊 TOTAL DE REGISTROS: {total_registros:,}")
        
        # Identificar duplicados por clave (time, cName, cReportGroup)
        cursor.execute(f"""
            SELECT 
                time, cName, cReportGroup, COUNT(*) as cantidad
            FROM [dbo].[{SQL_TABLE}]
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
            for i, (time, cName, cReportGroup, cantidad) in enumerate(duplicados[:10]):
                print(f"  {i+1}. {time} | {cName} | {cReportGroup} | {cantidad} registros")
            
            if len(duplicados) > 10:
                print(f"  ... y {len(duplicados) - 10} combinaciones más")
            
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
        cursor.execute(f"""
            SELECT COUNT(*) - COUNT(DISTINCT CONCAT(CAST(time AS VARCHAR(50)), '|', ISNULL(cName, ''), '|', ISNULL(cReportGroup, '')))
            FROM [dbo].[{SQL_TABLE}]
        """)
        registros_a_eliminar = cursor.fetchone()[0]
        
        if registros_a_eliminar == 0:
            print("✅ No hay duplicados para eliminar")
            return 0
        
        print(f"🗑️  Eliminando {registros_a_eliminar:,} registros duplicados...")
        
        # Eliminar duplicados manteniendo solo el registro más reciente por cada combinación única
        cursor.execute(f"""
            WITH Duplicados AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY time, cName, cReportGroup
                        ORDER BY fechaCarga DESC
                    ) as rn
                FROM [dbo].[{SQL_TABLE}]
            )
            DELETE FROM Duplicados 
            WHERE rn > 1
        """)
        
        registros_eliminados = cursor.rowcount
        cnx.commit()
        
        print(f"✅ DUPLICADOS ELIMINADOS: {registros_eliminados:,} registros")
        
        # Verificar resultado
        cursor.execute(f"SELECT COUNT(*) FROM [dbo].[{SQL_TABLE}]")
        registros_finales = cursor.fetchone()[0]
        print(f"📊 REGISTROS FINALES: {registros_finales:,}")
        
        return registros_eliminados

def mostrar_resumen_final():
    """Mostrar resumen final después de la limpieza"""
    with conectar_sql() as cnx:
        cursor = cnx.cursor()
        
        print("\n=== RESUMEN FINAL ===")
        
        # Total de registros
        cursor.execute(f"SELECT COUNT(*) FROM [dbo].[{SQL_TABLE}]")
        total_final = cursor.fetchone()[0]
        print(f"📊 TOTAL REGISTROS FINALES: {total_final:,}")
        
        # Registros por cola
        cursor.execute(f"""
            SELECT 
                cReportGroup,
                COUNT(*) as registros
            FROM [dbo].[{SQL_TABLE}]
            GROUP BY cReportGroup
            ORDER BY registros DESC
        """)
        
        colas = cursor.fetchall()
        print(f"\n📈 REGISTROS POR COLA:")
        for cola, registros in colas:
            print(f"  • {cola}: {registros:,} registros")
        
        # Verificar que no hay duplicados
        cursor.execute(f"""
            SELECT COUNT(*) - COUNT(DISTINCT CONCAT(CAST(time AS VARCHAR(50)), '|', ISNULL(cName, ''), '|', ISNULL(cReportGroup, '')))
            FROM [dbo].[{SQL_TABLE}]
        """)
        duplicados_restantes = cursor.fetchone()[0]
        
        if duplicados_restantes == 0:
            print(f"\n✅ VERIFICACIÓN: No hay duplicados restantes")
        else:
            print(f"\n⚠️  VERIFICACIÓN: Aún hay {duplicados_restantes} duplicados")

def main():
    print(f"{datetime.now()} – 🚀 INICIANDO LIMPIEZA DE DUPLICADOS EN {SQL_TABLE}")
    
    try:
        # Analizar duplicados
        hay_duplicados, total_duplicados, a_eliminar = analizar_duplicados()
        
        if hay_duplicados:
            print(f"\n¿Deseas eliminar {a_eliminar:,} registros duplicados? (s/n): ", end="")
            respuesta = input().lower()
            
            if respuesta in ['s', 'si', 'sí', 'y', 'yes']:
                eliminados = limpiar_duplicados()
                print(f"\n🎉 LIMPIEZA COMPLETADA: {eliminados:,} registros eliminados")
                
                # Mostrar resumen final
                mostrar_resumen_final()
            else:
                print("❌ Limpieza cancelada por el usuario")
        else:
            print("✅ No es necesario limpiar duplicados")
            
    except Exception as e:
        print(f"❌ Error durante la limpieza: {e}")

if __name__ == "__main__":
    main() 