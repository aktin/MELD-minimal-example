SELECT nval_num AS temperature_celsius,
       CASE
           WHEN nval_num >= 38 THEN 1
           ELSE 0
           END  AS has_fever
FROM observation_fact
WHERE concept_cd = 'LOINC:8329-5'
  AND nval_num IS NOT NULL
  AND units_cd IN ('Cel', 'C', 'Celsius');
