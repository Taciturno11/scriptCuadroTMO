-- =====================================================
-- SCRIPT PARA LIMPIAR DUPLICADOS EN Cuadro_TMO2
-- Elimina registros duplicados manteniendo solo el más reciente
-- =====================================================

-- PASO 1: Verificar cuántos duplicados hay
SELECT 
    COUNT(*) - COUNT(DISTINCT CONCAT(CAST(time AS VARCHAR(50)), '|', ISNULL(cName, ''), '|', ISNULL(cReportGroup, ''))) as Total_Duplicados
FROM [dbo].[Cuadro_TMO2];

-- PASO 2: Ver ejemplos de duplicados
SELECT TOP 10
    time, 
    cName, 
    cReportGroup, 
    COUNT(*) as cantidad_duplicados
FROM [dbo].[Cuadro_TMO2]
GROUP BY time, cName, cReportGroup
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

-- PASO 3: ELIMINAR DUPLICADOS (mantener solo el registro más reciente por fechaCarga)
WITH Duplicados AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY time, cName, cReportGroup
            ORDER BY fechaCarga DESC
        ) as rn
    FROM [dbo].[Cuadro_TMO2]
)
DELETE FROM Duplicados 
WHERE rn > 1;

-- PASO 4: Verificar resultado final
SELECT COUNT(*) as Registros_Finales
FROM [dbo].[Cuadro_TMO2];

-- PASO 5: Verificar que no hay duplicados restantes
SELECT 
    COUNT(*) - COUNT(DISTINCT CONCAT(CAST(time AS VARCHAR(50)), '|', ISNULL(cName, ''), '|', ISNULL(cReportGroup, ''))) as Duplicados_Restantes
FROM [dbo].[Cuadro_TMO2]; 