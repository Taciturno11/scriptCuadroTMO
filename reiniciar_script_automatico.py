# reiniciar_script_automatico.py
import subprocess
import sys
import time
from datetime import datetime

def reiniciar_script_automatico():
    """Reinicia el script automático después del reset"""
    print(f"{datetime.now()} – 🚀 REINICIANDO SCRIPT AUTOMÁTICO")
    print(f"{datetime.now()} – 📅 Después del reset completo")
    
    try:
        # Ejecutar el script automático
        print(f"{datetime.now()} – ▶️  Ejecutando cargar_datos_desde_agosto.py")
        
        # Usar subprocess para ejecutar el script
        proceso = subprocess.Popen([
            sys.executable, 
            "cargar_datos_desde_agosto.py"
        ], 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        text=True)
        
        print(f"{datetime.now()} – ✅ Script automático iniciado")
        print(f"{datetime.now()} – 📊 PID del proceso: {proceso.pid}")
        
        # Mostrar las primeras líneas de salida
        print(f"{datetime.now()} – 🔍 Verificando salida inicial...")
        
        # Esperar un poco y verificar si está funcionando
        time.sleep(5)
        
        if proceso.poll() is None:
            print(f"{datetime.now()} – ✅ Script automático ejecutándose correctamente")
            print(f"{datetime.now()} – 🎯 El script procesará datos cada hora automáticamente")
        else:
            print(f"{datetime.now()} – ⚠️  Script terminó prematuramente")
            stdout, stderr = proceso.communicate()
            if stderr:
                print(f"{datetime.now()} – ❌ Error: {stderr}")
        
        return proceso
        
    except Exception as e:
        print(f"{datetime.now()} – ❌ Error iniciando script: {e}")
        return None

def verificar_estado_script():
    """Verifica el estado del script automático"""
    print(f"{datetime.now()} – 🔍 VERIFICANDO ESTADO DEL SCRIPT")
    
    try:
        # Verificar si hay procesos de Python ejecutándose
        resultado = subprocess.run([
            "tasklist", 
            "/FI", 
            "IMAGENAME eq python.exe"
        ], 
        capture_output=True, 
        text=True)
        
        if "python.exe" in resultado.stdout:
            print(f"{datetime.now()} – ✅ Procesos de Python activos encontrados")
            print(f"{datetime.now()} – 📊 Script automático probablemente ejecutándose")
        else:
            print(f"{datetime.now()} – ⚠️  No se encontraron procesos de Python activos")
            
    except Exception as e:
        print(f"{datetime.now()} – ❌ Error verificando estado: {e}")

def main():
    print(f"{datetime.now()} – 🎯 REINICIO POST RESET")
    print(f"{datetime.now()} – 📋 Pasos a seguir:")
    print(f"   1. ✅ Verificar datos cargados")
    print(f"   2. 🚀 Reiniciar script automático")
    print(f"   3. 🔍 Verificar estado")
    
    # Confirmar con el usuario
    respuesta = input(f"\n¿Quieres reiniciar el script automático? (s/n): ").lower()
    
    if respuesta in ['s', 'si', 'sí', 'y', 'yes']:
        proceso = reiniciar_script_automatico()
        
        if proceso:
            print(f"\n{'='*60}")
            print(f"REINICIO COMPLETADO")
            print(f"{'='*60}")
            print(f"✅ Script automático reiniciado")
            print(f"✅ Procesará datos cada hora")
            print(f"✅ Incluirá todos los agentes (PSGGPERL)")
            print(f"✅ Sin duplicados")
            print(f"\n🎯 PRÓXIMOS PASOS:")
            print(f"   - El script se ejecutará automáticamente cada hora")
            print(f"   - Puedes monitorear los logs en la consola")
            print(f"   - Verificar datos con: python verificar_post_reset.py")
        else:
            print(f"{datetime.now()} – ❌ Error reiniciando script")
    else:
        print(f"{datetime.now()} – ❌ Reinicio cancelado por el usuario")

if __name__ == "__main__":
    main() 