-- =====================================================
-- SCRIPT PARA CREAR TABLA Cuadro_TMO2
-- NOMBRES DE COLUMNAS EXACTOS DE GRAFANA
-- =====================================================

-- Crear la tabla Cuadro_TMO2 con nombres exactos de Grafana
CREATE TABLE [dbo].[Cuadro_TMO2] (
    [time] datetime NOT NULL,
    [cName] varchar(50) NOT NULL,
    [cReportGroup] varchar(50) NOT NULL,
    [Recibidas] int NULL,
    [Respondidas] int NULL,
    [Abandonadas] int NULL,
    [Abandonadas 5s] int NULL,
    [TMO s tHablado/int ] float NULL,
    [% Hold] float NULL,
    [TME Respondida] float NULL,
    [TME Abandonada] float NULL,
    [Tiempo Disponible H] float NULL,
    [Tiempo Hablado  H] float NULL,
    [Tiempo Recarga  H] float NULL,
    [Tiempo ACW  H] float NULL,
    [Tiempo No Disponible  H] float NULL,
    [Tiempo Total LoggedIn] float NULL,
    [Hora ACD] float NULL,
    [% Disponible] float NULL,
    [% Hablado] float NULL,
    [% Recarga] float NULL,
    [Int. Salientes manuales] int NULL,
    [Tiempo en int. salientes manuales (H)] float NULL,
    [TMO s Int. Salientes manuales] float NULL,
    [fechaCarga] datetime DEFAULT GETDATE()
);

-- Crear índice para mejorar performance en consultas por tiempo
CREATE INDEX IX_Cuadro_TMO2_time 
ON [dbo].[Cuadro_TMO2] (time);

-- Crear índice para mejorar performance en consultas por cola
CREATE INDEX IX_Cuadro_TMO2_cReportGroup 
ON [dbo].[Cuadro_TMO2] (cReportGroup);

-- Crear índice compuesto para mejorar performance en consultas por tiempo y cola
CREATE INDEX IX_Cuadro_TMO2_time_cReportGroup 
ON [dbo].[Cuadro_TMO2] (time, cReportGroup);

-- Crear índice compuesto para mejorar performance en consultas por tiempo, nombre y cola
CREATE INDEX IX_Cuadro_TMO2_time_cName_cReportGroup 
ON [dbo].[Cuadro_TMO2] (time, cName, cReportGroup);

-- Verificar que la tabla se creó correctamente
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'dbo' 
AND TABLE_NAME = 'Cuadro_TMO2'
ORDER BY ORDINAL_POSITION;

-- Mostrar los índices creados
SELECT 
    i.name as IndexName,
    i.type_desc as IndexType,
    STRING_AGG(c.name, ', ') as Columns
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.object_id = OBJECT_ID('dbo.Cuadro_TMO2')
GROUP BY i.name, i.type_desc
ORDER BY i.type_desc, i.name; 