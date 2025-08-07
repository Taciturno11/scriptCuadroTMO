-- =====================================================
-- CONSULTAS PARA VERIFICAR DUPLICADOS POR CLAVE
-- (time, cName, cReportGroup)
-- =====================================================

-- CONSULTA 1: Ver cuántos registros tienen la misma combinación (time, cName, cReportGroup)
SELECT 
    time, 
    cName, 
    cReportGroup, 
    COUNT(*) as cantidad_registros
FROM [dbo].[Cuadro_TMO2]
GROUP BY time, cName, cReportGroup
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC, time DESC;

-- CONSULTA 2: Ver ejemplos específicos de duplicados
SELECT TOP 20
    time, 
    cName, 
    cReportGroup,
    COUNT(*) as cantidad_duplicados
FROM [dbo].[Cuadro_TMO2]
GROUP BY time, cName, cReportGroup
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC, time DESC;

-- CONSULTA 3: Ver todos los registros duplicados (mostrar todas las filas)
WITH Duplicados AS (
    SELECT 
        time, 
        cName, 
        cReportGroup,
        COUNT(*) as cantidad
    FROM [dbo].[Cuadro_TMO2]
    GROUP BY time, cName, cReportGroup
    HAVING COUNT(*) > 1
)
SELECT 
    c.time,
    c.cName,
    c.cReportGroup,
    c.Recibidas,
    c.Respondidas,
    c.Abandonadas,
    c.fechaCarga
FROM [dbo].[Cuadro_TMO2] c
INNER JOIN Duplicados d 
    ON c.time = d.time 
    AND c.cName = d.cName 
    AND c.cReportGroup = d.cReportGroup
ORDER BY c.time DESC, c.cName, c.cReportGroup;

-- CONSULTA 4: Contar total de registros duplicados
SELECT 
    COUNT(*) - COUNT(DISTINCT CONCAT(CAST(time AS VARCHAR(50)), '|', ISNULL(cName, ''), '|', ISNULL(cReportGroup, ''))) as Total_Duplicados_Por_Clave
FROM [dbo].[Cuadro_TMO2];

-- CONSULTA 5: Resumen de duplicados por cola
SELECT 
    cReportGroup,
    COUNT(*) as total_registros,
    COUNT(DISTINCT CONCAT(CAST(time AS VARCHAR(50)), '|', ISNULL(cName, ''), '|', ISNULL(cReportGroup, ''))) as registros_unicos,
    COUNT(*) - COUNT(DISTINCT CONCAT(CAST(time AS VARCHAR(50)), '|', ISNULL(cName, ''), '|', ISNULL(cReportGroup, ''))) as duplicados
FROM [dbo].[Cuadro_TMO2]
GROUP BY cReportGroup
ORDER BY duplicados DESC; 