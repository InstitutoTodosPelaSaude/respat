{{ config(materialized='table') }}

SELECT 
	lab_id,
	test_kit,
	pathogen, 
	"result",
	count(*)
FROM {{ ref("matrices_01_unpivot_combined") }}
WHERE "result" <> 'NT'
GROUP BY lab_id, test_kit, pathogen, "result"
ORDER BY lab_id, test_kit, pathogen, "result"