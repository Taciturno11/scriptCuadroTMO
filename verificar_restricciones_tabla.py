# verificar_restricciones_tabla.py
import pyodbc

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

def verificar_restricciones():
    with conectar_sql() as cnx:
        cursor = cnx.cursor()
        
        print("=== RESTRICCIONES DE LA TABLA Cuadro_TMO ===")
        
        # Verificar clave primaria
        cursor.execute("""
            SELECT 
                COLUMN_NAME,
                CONSTRAINT_NAME,
                CONSTRAINT_TYPE
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc 
                ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            WHERE tc.TABLE_SCHEMA = 'dbo' 
            AND tc.TABLE_NAME = 'Cuadro_TMO'
            AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
        """)
        
        restricciones = cursor.fetchall()
        
        if restricciones:
            print("🔑 RESTRICCIONES ENCONTRADAS:")
            for col, constraint, tipo in restricciones:
                print(f"  - {tipo}: {constraint} en columna {col}")
        else:
            print("⚠️  NO HAY RESTRICCIONES DE CLAVE PRIMARIA O ÚNICA")
            print("   Los duplicados se detectarán por otras restricciones o por error de inserción")
        
        # Verificar índices
        cursor.execute("""
            SELECT 
                i.name as IndexName,
                i.type_desc as IndexType,
                STRING_AGG(c.name, ', ') as Columns
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE i.object_id = OBJECT_ID('dbo.Cuadro_TMO')
            GROUP BY i.name, i.type_desc
            ORDER BY i.type_desc, i.name
        """)
        
        indices = cursor.fetchall()
        
        if indices:
            print("\n📊 ÍNDICES ENCONTRADOS:")
            for nombre, tipo, columnas in indices:
                print(f"  - {tipo}: {nombre} en columnas ({columnas})")
        else:
            print("\n⚠️  NO HAY ÍNDICES DEFINIDOS")

if __name__ == "__main__":
    verificar_restricciones() 