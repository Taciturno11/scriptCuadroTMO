# verificar_fecha_carga.py
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

def verificar_fecha_carga():
    with conectar_sql() as cnx:
        cursor = cnx.cursor()
        
        # Verificar si la columna fechaCarga existe
        cursor.execute("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = 'dbo' 
            AND TABLE_NAME = 'Cuadro_TMO'
            AND COLUMN_NAME = 'fechaCarga'
        """)
        
        resultado = cursor.fetchone()
        
        if resultado:
            print("✅ La columna 'fechaCarga' YA EXISTE en la tabla Cuadro_TMO")
            return True
        else:
            print("❌ La columna 'fechaCarga' NO EXISTE en la tabla Cuadro_TMO")
            return False

def agregar_columna_fecha_carga():
    with conectar_sql() as cnx:
        cursor = cnx.cursor()
        
        try:
            # Agregar la columna fechaCarga
            cursor.execute("""
                ALTER TABLE [dbo].[Cuadro_TMO] 
                ADD fechaCarga datetime DEFAULT GETDATE()
            """)
            cnx.commit()
            print("✅ Columna 'fechaCarga' agregada exitosamente")
            return True
        except Exception as e:
            print(f"❌ Error al agregar la columna: {e}")
            return False

if __name__ == "__main__":
    print("=== VERIFICACIÓN DE COLUMNA fechaCarga ===")
    
    if not verificar_fecha_carga():
        print("\n¿Deseas agregar la columna fechaCarga? (s/n): ", end="")
        respuesta = input().lower()
        
        if respuesta in ['s', 'si', 'sí', 'y', 'yes']:
            agregar_columna_fecha_carga()
        else:
            print("❌ No se agregó la columna fechaCarga")
    else:
        print("✅ La columna ya existe, no es necesario agregarla") 