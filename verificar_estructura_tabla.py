# verificar_estructura_tabla.py
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

def verificar_estructura():
    with conectar_sql() as cnx:
        cursor = cnx.cursor()
        
        # Obtener información de columnas
        cursor.execute("""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = 'dbo' 
            AND TABLE_NAME = 'Cuadro_TMO'
            ORDER BY ORDINAL_POSITION
        """)
        
        print("=== ESTRUCTURA DE LA TABLA Cuadro_TMO ===")
        print("COLUMN_NAME | DATA_TYPE | IS_NULLABLE | MAX_LENGTH")
        print("-" * 50)
        
        columnas = []
        for row in cursor.fetchall():
            column_name, data_type, is_nullable, max_length = row
            columnas.append(column_name)
            max_len_str = str(max_length) if max_length else "NULL"
            print(f"{column_name:<25} | {data_type:<10} | {is_nullable:<11} | {max_len_str}")
        
        print(f"\nTotal de columnas: {len(columnas)}")
        print("\nNombres exactos de columnas:")
        for i, col in enumerate(columnas, 1):
            print(f"{i:2d}. {col}")
        
        return columnas

if __name__ == "__main__":
    try:
        columnas = verificar_estructura()
    except Exception as e:
        print(f"Error: {e}") 