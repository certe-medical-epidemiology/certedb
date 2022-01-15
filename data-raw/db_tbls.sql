-- ===================================================================== --
--  An R package by Certe:                                               --
--  https://github.com/certe-medical-epidemiology                        --
--                                                                       --
--  Licensed AS GPL-v2.0.                                                --
--                                                                       --
--  Developed at non-profit organisation Certe Medical Diagnostics &     --
--  Advice, department of Medical Epidemiology.                          --
--                                                                       --
--  This R package is free software; you can freely use and distribute   --
--  it for both personal and commercial purposes under the terms of the  --
--  GNU General Public License version 2.0 (GNU GPL-2), AS published by  --
--  the Free Software Foundation.                                        --
--                                                                       --
--  We created this package for both routine data analysis and academic  --
--  research and it was publicly released in the hope that it will be    --
--  useful, but it comes WITHOUT ANY WARRANTY OR LIABILITY.              --
-- ===================================================================== --

-- from https://dataedo.com/kb/query/mysql/list-table-columns-in-database
-- on 2022-01-14
SELECT tab.table_schema AS database_schema,
    tab.table_name AS table_name,
    col.ordinal_position AS column_id,
    col.column_name AS column_name,
    col.data_type AS data_type,
    CASE WHEN col.numeric_precision is NOT NULL
        THEN col.numeric_precision
        ELSE col.character_maximum_length END AS max_length,
    CASE WHEN col.datetime_precision is NOT NULL
        THEN col.datetime_precision
        WHEN col.numeric_scale is NOT NULL
        THEN col.numeric_scale
            ELSE 0 END AS 'precision'
FROM information_schema.tables AS tab
    INNER JOIN information_schema.columns AS col
        ON col.table_schema = tab.table_schema
        AND col.table_name = tab.table_name
WHERE tab.table_type = 'BASE TABLE'
    AND tab.table_schema NOT IN ('information_schema','mysql',
        'performance_schema','sys')
    -- uncomment line below for current database only
    -- AND tab.table_schema = database() 
    -- uncomment line below and provide specific database name
    -- AND tab.table_schema = 'your_database_name' 
ORDER BY tab.table_name,
    col.ordinal_position;
