-- =====================================================
-- SCRIPT PARA ELIMINAR DUPLICADOS EXACTOS
-- Elimina registros que sean idénticos en TODOS los campos
-- =====================================================

-- PASO 1: Verificar cuántos duplicados exactos hay
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
    ) as Duplicados_Exactos
FROM [dbo].[Cuadro_TMO]

-- PASO 2: Ver ejemplos de duplicados exactos
SELECT 
    time, cName, cReportGroup, Recibidas, Respondidas, Abandonadas,
    COUNT(*) as Cantidad_Duplicados
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
ORDER BY COUNT(*) DESC

-- PASO 3: Eliminar duplicados exactos (mantener solo uno de cada conjunto)
WITH DuplicadosExactos AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                time, cName, cReportGroup, Recibidas, Respondidas, Abandonadas,
                [Abandonadas 5s], [TMO s tHablado/int], [% Hold], [TME Respondida],
                [TME Abandonada], [Tiempo Disponible H], [Tiempo Hablado  H],
                [Tiempo Recarga  H], [Tiempo ACW  H], [Tiempo No Disponible  H],
                [Tiempo Total LoggedIn], [Hora ACD], [% Disponible], [% Hablado],
                [% Recarga], [Int  Salientes manuales], [Tiempo en int  salientes manuales (H)],
                [TMO s Int  Salientes manuales]
            ORDER BY fechaCarga DESC
        ) as rn
    FROM [dbo].[Cuadro_TMO]
)
DELETE FROM DuplicadosExactos 
WHERE rn > 1

-- PASO 4: Verificar resultado final
SELECT COUNT(*) as Registros_Finales
FROM [dbo].[Cuadro_TMO] 