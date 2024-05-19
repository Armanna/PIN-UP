WITH amounts AS (
  SELECT pe_description,
         COUNT(record_id) AS record_count
  FROM los_angeles_restaurant_health_inspections
  WHERE facility_name LIKE '%TEA%'
    OR facility_name LIKE '%CAFE%'
    OR facility_name LIKE '%JUICE%'
  GROUP BY pe_description
),
   
ranks AS (
  SELECT pe_description,
         DENSE_RANK() OVER(ORDER BY record_count DESC) AS rank_num
  FROM amounts
)

SELECT facility_name FROM los_angeles_restaurant_health_inspections
where pe_description in (select pe_description from ranks where rank_num = 3)
and (facility_name LIKE '%TEA%'
    OR facility_name LIKE '%CAFE%'
    OR facility_name LIKE '%JUICE%')