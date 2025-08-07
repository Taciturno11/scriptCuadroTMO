-- =====================================================
-- CONSULTAS PARA VERIFICAR DUPLICADOS EXACTOS
-- =====================================================

-- CONSULTA 1: Ver cuántos registros tienen la misma combinación (time, cName, cReportGroup)
SELECT 
    time, 
    cName, 
    cReportGroup, 
    COUNT(*) as cantidad_registros
FROM [dbo].[Cuadro_TMO]
GROUP BY time, cName, cReportGroup
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

-- CONSULTA 2: Ver duplicados exactos (mismas columnas en todas las filas)
SELECT 
    time, 
    cName, 
    cReportGroup,
    COUNT(*) as cantidad_duplicados_exactos
FROM [dbo].[Cuadro_TMO]
GROUP BY 
    time, cName, cReportGroup, Recibidas, Respondidas, Abandonadas,
    [Abandonadas 5s], [TMO s tHablado/int], [% Hold], [TME Respondida],
    [TME Abandonada], [Tiempo Disponible H], [Tiempo Hablado  H],
    [Tiempo Recarga  H], [Tiempo ACW  H], [Tiempo No Disponible  H],
    [Tiempo Total LoggedIn], [Hora ACD], [% Disponible], [% Hablado],
    [% Recarga], [Int  Salientes manuales], [Tiempo en int  salientes manuales (H)],
    [TMO s Int  Salientes manuales]
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

-- CONSULTA 3: Ver ejemplos de duplicados exactos
SELECT TOP 10
    time, 
    cName, 
    cReportGroup,
    Recibidas,
    Respondidas,
    Abandonadas,
    COUNT(*) as cantidad
FROM [dbo].[Cuadro_TMO]
GROUP BY 
    time, cName, cReportGroup, Recibidas, Respondidas, Abandonadas,
    [Abandonadas 5s], [TMO s tHablado/int], [% Hold], [TME Respondida],
    [TME Abandonada], [Tiempo Disponible H], [Tiempo Hablado  H],
    [Tiempo Recarga  H], [Tiempo ACW  H], [Tiempo No Disponible  H],
    [Tiempo Total LoggedIn], [Hora ACD], [% Disponible], [% Hablado],
    [% Recarga], [Int  Salientes manuales], [Tiempo en int  salientes manuales (H)],
    [TMO s Int  Salientes manuales]
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

-- CONSULTA 4: Contar total de duplicados exactos
SELECT 
    COUNT(*) - COUNT(DISTINCT 
        CONCAT(
            CAST(time AS VARCHAR(50)), '|',
            ISNULL(cName, ''), '|',
            ISNULL(cReportGroup, ''), '|',
            ISNULL(CAST(Recibidas AS VARCHAR(10)), ''), '|',
            ISNULL(CAST(Respondidas AS VARCHAR(10)), ''), '|',
            ISNULL(CAST(Abandonadas AS VARCHAR(10)), ''), '|',
            ISNULL(CAST([Abandonadas 5s] AS VARCHAR(10)), ''), '|',
            ISNULL(CAST([TMO s tHablado/int] AS VARCHAR(10)), ''), '|',
            ISNULL([% Hold], ''), '|',
            ISNULL(CAST([TME Respondida] AS VARCHAR(10)), ''), '|',
            ISNULL(CAST([TME Abandonada] AS VARCHAR(10)), ''), '|',
            ISNULL(CAST([Tiempo Disponible H] AS VARCHAR(10)), ''), '|',
            ISNULL(CAST([Tiempo Hablado  H] AS VARCHAR(10)), ''), '|',
            ISNULL(CAST([Tiempo Recarga  H] AS VARCHAR(10)), ''), '|',
            ISNULL(CAST([Tiempo ACW  H] AS VARCHAR(10)), ''), '|',
            ISNULL(CAST([Tiempo No Disponible  H] AS VARCHAR(10)), ''), '|',
            ISNULL(CAST([Tiempo Total LoggedIn] AS VARCHAR(10)), ''), '|',
            ISNULL(CAST([Hora ACD] AS VARCHAR(10)), ''), '|',
            ISNULL([% Disponible], ''), '|',
            ISNULL([% Hablado], ''), '|',
            ISNULL([% Recarga], ''), '|',
            ISNULL(CAST([Int  Salientes manuales] AS VARCHAR(10)), ''), '|',
            ISNULL(CAST([Tiempo en int  salientes manuales (H)] AS VARCHAR(10)), ''), '|',
            ISNULL(CAST([TMO s Int  Salientes manuales] AS VARCHAR(10)), '')
        )
    ) as Total_Duplicados_Exactos
FROM [dbo].[Cuadro_TMO]; 