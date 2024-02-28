{{ config(materialized='table') }}

with source_table as (
    select * from (
        VALUES(1, 'red'),
            (2, 'orange'),
            (5, 'yellow'),
            (10, 'green'),
            (11, 'blue'),
            (12, 'indigo'),
            (20, 'violet'))
        AS Colors(Id, Value)
    )
)

SELECT * from source_table;