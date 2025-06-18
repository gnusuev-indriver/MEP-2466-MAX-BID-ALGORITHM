WITH rider_data AS (SELECT city_id              AS city_id,
                           timezone             AS timezone,
                           type_name            AS type_name,
                           uuid                 AS order_uuid,
                           CAST(NULL AS STRING) AS tender_uuid,
                           'rider_price'        AS price_type,
                           payment_price_value  AS price,
                           price_highrate_value AS price_highrate_value,
                           accepted_tender_uuid AS accepted_tender_uuid,
                           AtoB_seconds         AS AtoB_seconds,
                           CAST(NULL AS INT64)  AS eta,
                           modified_at          AS modified_at_utc,
                           order_done           AS order_done
                    FROM `indriver-e6e40.ods_new_order_rh_cdc.order_global_strm` t1
                             LEFT JOIN (SELECT order_uuid,
                                               MAX(timezone)                                                            AS timezone,
                                               MAX(CASE WHEN driverdone_timestamp IS NOT NULL THEN tender_uuid END)     AS accepted_tender_uuid,
                                               MAX(CASE WHEN driverdone_timestamp IS NOT NULL THEN true ELSE false END) AS order_done,
                                               MAX(duration_in_seconds)                                                 AS AtoB_seconds
                                        FROM `indriver-e6e40.emart.incity_detail`
                                        WHERE true
                                          AND order_uuid IS NOT NULL
                                          AND created_date_order_part BETWEEN
                                            DATE_SUB('{{date_range.start}}' , INTERVAL 1 DAY)
                                            AND DATE_ADD('{{date_range.end}}' , INTERVAL 1 DAY)
                                        GROUP BY order_uuid) t2
                                       ON t1.uuid = t2.order_uuid
                    WHERE true
                      AND order_uuid IS NOT NULL
                      AND status = 'ORDER_STATUS_ACTIVE'
                      AND AtoB_seconds > 0
                      AND DATE(created_at) BETWEEN
                        DATE_SUB('{{date_range.start}}' , INTERVAL 1 DAY)
                        AND DATE_ADD('{{date_range.end}}' , INTERVAL 1 DAY)
                      AND city_id = 4180
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY uuid, payment_price_value ORDER BY modified_at) = 1),

     bid_data AS (SELECT DISTINCT CAST(NULL AS INT64)                                      AS city_id,
                                  CAST(NULL AS STRING)                                     AS timezone,
                                  CAST(NULL AS STRING)                                     AS type_name,
                                  order_uuid                                               AS order_uuid,
                                  uuid                                                     AS tender_uuid,
                                  'bid_price'                                              AS price_type,
                                  price                                                    AS price,
                                  CAST(NULL AS INT64)                                      AS price_highrate_value,
                                  CAST(NULL AS STRING)                                     AS accepted_tender_uuid,
                                  CAST(NULL AS INT64)                                      AS AtoB_seconds,
                                  SAFE_CAST(SUBSTR(eta, 1, STRPOS(eta, 's') - 1) AS INT64) AS eta,
                                  modified_at                                              AS modified_at_utc,
                                  CAST(NULL AS BOOL)                                       AS order_done,
                  FROM `indriver-e6e40.ods_new_order_rh_cdc.bid_global_strm`
                  WHERE true
                    AND order_uuid IS NOT NULL
                    AND uuid IS NOT NULL
                    AND order_uuid IN (SELECT order_uuid FROM rider_data)
                    AND status = 'BID_STATUS_ACTIVE'
                    AND DATE(created_at) BETWEEN
                      DATE_SUB('{{date_range.start}}' , INTERVAL 1 DAY)
                      AND DATE_ADD('{{date_range.end}}' , INTERVAL 1 DAY)),

     my_strm AS (SELECT city_id,
                        timezone,
                        type_name,
                        order_uuid,
                        tender_uuid,
                        price_type,
                        price,
                        price_highrate_value,
                        accepted_tender_uuid,
                        AtoB_seconds,
                        eta,
                        modified_at_utc,
                        order_done
                 FROM (SELECT *
                       FROM rider_data
                       UNION ALL
                       SELECT *
                       FROM bid_data)
                 ORDER BY order_uuid, modified_at_utc),

     my_strm_full AS (SELECT LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN city_id END IGNORE NULLS)
                                        OVER w        AS city_id,
                             LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN timezone END IGNORE NULLS)
                                        OVER w        AS timezone,
                             LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN type_name END IGNORE NULLS)
                                        OVER w        AS type_name,
                             order_uuid,
                             tender_uuid,
                             price_type,
                             price,
                             LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN price_highrate_value END IGNORE
                                        NULLS) OVER w AS price_highrate_value,
                             LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN price END IGNORE
                                        NULLS) OVER w AS start_price_value,
                             LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN accepted_tender_uuid END IGNORE
                                        NULLS) OVER w AS accepted_tender_uuid,
                             LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN AtoB_seconds END IGNORE
                                        NULLS) OVER w AS AtoB_seconds,
                             eta,
                             modified_at_utc,
                             LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN order_done END IGNORE
                                        NULLS) OVER w AS order_done
                      FROM my_strm
                      WINDOW w AS (
                              PARTITION BY order_uuid
                              ORDER BY modified_at_utc
                              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )),

     options_assign AS (SELECT city_id,
                               type_name,
                               order_uuid,
                               tender_uuid,
                               price_type,
                               accepted_tender_uuid,
                               order_done,
                               DATETIME(TIMESTAMP(modified_at_utc), timezone)               AS modified_at_local,
                               SAFE_DIVIDE(SAFE_DIVIDE(price, (AtoB_seconds + eta)),
                                           SAFE_DIVIDE(GREATEST(price_highrate_value, start_price_value),
                                                       (AtoB_seconds + {{pickup_eta_minutes}} * 60))) AS ratio,
                                                       
                                                       CASE
                        WHEN REGEXP_CONTAINS(LOWER(type_name), r'comfort') THEN 'COMFORT'
                        WHEN REGEXP_CONTAINS(LOWER(type_name), r'trimoto') THEN 'TRIMOTOS'
                        WHEN REGEXP_CONTAINS(LOWER(type_name), r'moto|bike') THEN 'MOTO, BIKE'
                        WHEN REGEXP_CONTAINS(LOWER(type_name), r'ac|a/c') THEN 'RIDE_AC'
                        WHEN REGEXP_CONTAINS(LOWER(type_name), r'mini') THEN 'RIDE_MINI'
                        WHEN REGEXP_CONTAINS(LOWER(type_name), r'econom|ride') THEN 'ECONOM'
                        WHEN REGEXP_CONTAINS(LOWER(type_name), r'rickshaw|trike|tuk|bajaj') THEN 'RICKSHAW'
                        WHEN REGEXP_CONTAINS(LOWER(type_name), r'6-seater|6_seater|xl') THEN '6 SEATER, XL'
                        ELSE type_name
                        END as type_name_agg
                        FROM my_strm_full)

SELECT -- t1.city_id,
       MAX(city) AS city,
       t1.city_id,
       MAX(country) AS country,
       type_name,
       COUNT(IF(order_done = true AND tender_uuid = accepted_tender_uuid AND
                ratio > 1 + {{alpha}}, price_type, NULL))              AS rides_badbid_cnt,
       COUNT(DISTINCT IF(order_done = true, order_uuid, NULL))      AS rides_cnt,
       ROUND(SAFE_DIVIDE(COUNT(IF(ratio > 1 + {{alpha}}, price_type, NULL)),
                   COUNT(price_type)), 3)                               AS badbids_share,
       ROUND(SAFE_DIVIDE(SUM(IF(ratio > 1 + {{alpha}}, ratio, NULL)),
                   COUNT(IF(ratio > 1 + {{alpha}}, price_type, NULL))), 3) AS badbid_ratio_avg,
       ROUND(SAFE_DIVIDE(COUNT(IF(order_done = true AND tender_uuid = accepted_tender_uuid AND
                            ratio > 1 + {{alpha}}, order_uuid, NULL)),
                   COUNT(DISTINCT IF(order_done = true, order_uuid, NULL))), 3)
                                                                    AS rides_at_risk,
    --   COUNT(IF(order_done = true AND tender_uuid = accepted_tender_uuid, order_uuid, NULL)) AS rides_cnt_2,
    --   max(type_name_agg),
    --   max(order_type_agg_agg),
    --   max(rides_count_abacus)
FROM options_assign t1
LEFT JOIN (SELECT DISTINCT tc.id AS city_id, 
                --   CONCAT(tc.name, ' (', tc.id, ')') AS city, 
                  tc.name AS city, 
                  CONCAT(tcc.name) AS country
             FROM `indriver-e6e40.ods_geo_config.tbl_city` tc
             JOIN `indriver-e6e40.ods_geo_config.tbl_country` tcc 
             ON tc.country_id = tcc.id) t2
            ON t1.city_id = t2.city_id
-- LEFT JOIN (select city_id, CASE
--                       WHEN REGEXP_CONTAINS(LOWER(order_type_agg), r'comfort') THEN 'COMFORT'
--                       WHEN REGEXP_CONTAINS(LOWER(order_type_agg), r'trimoto') THEN 'TRIMOTOS'
--                       WHEN REGEXP_CONTAINS(LOWER(order_type_agg), r'moto|bike') THEN 'MOTO, BIKE'
--                       WHEN REGEXP_CONTAINS(LOWER(order_type_agg), r'ac|a/c') THEN 'RIDE_AC'
--                       WHEN REGEXP_CONTAINS(LOWER(order_type_agg), r'mini') THEN 'RIDE_MINI'
--                       WHEN REGEXP_CONTAINS(LOWER(order_type_agg), r'econom|ride') THEN 'ECONOM'
--                       WHEN REGEXP_CONTAINS(LOWER(order_type_agg), r'rickshaw|trike|tuk|bajaj') THEN 'RICKSHAW'
--                       WHEN REGEXP_CONTAINS(LOWER(order_type_agg), r'6-seater|6_seater|xl') THEN '6 SEATER, XL'
--                       ELSE order_type_agg
--                       END as order_type_agg_agg, 
--                       sum(metric_value) as rides_count_abacus
--             from `indriver-bi.abacus.abacus`
--             where date(metric_date) between DATE('{{date_range.start}}' ) AND DATE('{{date_range.end}}' )
--               and metric_name = 'rides_count'
--               and date_period = 'day'
--               and transport_type != 'any'
--               and order_type_agg != 'any'
--               and city_id != -1
--               and direction = 'incity'
--             group by city_id, order_type_agg_agg) t3
--             ON t1.city_id = t3.city_id AND t1.type_name_agg = t3.order_type_agg_agg
WHERE true
  AND price_type = 'bid_price'
  AND DATE(modified_at_local) BETWEEN DATE('{{date_range.start}}' ) AND DATE('{{date_range.end}}' )
GROUP BY city_id, type_name
HAVING rides_cnt > {{Min_rides}}
ORDER BY badbids_share DESC, badbid_ratio_avg DESC;