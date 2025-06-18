WITH
timezone_data AS (SELECT city_id,
                         timezone
                  FROM `indriver-e6e40.emart.incity_detail`
                  WHERE true
                    AND city_id = {{City_id}}
                    AND created_date_order_part = CURRENT_DATE()
                  LIMIT 1),

rider_data AS (SELECT t1.city_id                  AS city_id,
                      uuid                        AS order_uuid,
                      payment_price_value         AS price,
                      modified_at                 AS modified_at_utc,
                      'rider_price'               AS price_type,
                      price_highrate_value        AS price_highrate_value,
                      CAST(NULL AS INT64)         AS order_rn,
                      CAST(NULL AS INT64)         AS tender_rn,
                      CAST(NULL AS STRING)        AS tender_uuid,
                      CAST(NULL AS ARRAY <INT64>) AS available_prices,
                      CAST(NULL AS INT64)         AS contractor_id,
                      CAST(NULL AS INT64)         AS eta,
                      duration_in_seconds         AS duration_in_seconds,
                      CAST(NULL AS STRING)        AS accepted_status,
                      type_name                   AS order_type
               FROM `indriver-e6e40.ods_new_order_rh_cdc.order_global_strm` t1
                    LEFT JOIN timezone_data tz
                        ON t1.city_id = tz.city_id
                    LEFT JOIN (SELECT order_uuid,
                                      duration_in_seconds,
                               FROM `indriver-e6e40.emart.incity_detail`
                               WHERE true
                                 AND city_id = {{City_id}}
                                 AND created_date_order_part BETWEEN '{{date_range.start}}' AND '{{date_range.end}}') t2
                        ON t1.uuid = t2.order_uuid  
               WHERE true
                 AND TIMESTAMP_TRUNC(created_at, DAY) BETWEEN
                   TIMESTAMP_ADD(TIMESTAMP(PARSE_DATETIME('%Y-%m-%d', '{{date_range.start}}')), INTERVAL -1 DAY)
                   AND TIMESTAMP_ADD(TIMESTAMP(PARSE_DATETIME('%Y-%m-%d', '{{date_range.end}}')), INTERVAL 2 DAY)
                 AND created_at BETWEEN
                   TIMESTAMP_ADD(TIMESTAMP(PARSE_DATETIME('%Y-%m-%d', '{{date_range.start}}'), tz.timezone), INTERVAL 0 DAY)
                   AND TIMESTAMP_ADD(TIMESTAMP(PARSE_DATETIME('%Y-%m-%d', '{{date_range.end}}'), tz.timezone), INTERVAL 1
                                     DAY)
                 AND t1.city_id = {{City_id}}
                 AND ({{ Order Type }} LIKE '%*%' OR type_name IN ({{ Order Type }}))
               QUALIFY ROW_NUMBER() OVER (PARTITION BY uuid, payment_price_value ORDER BY modified_at) = 1),

accepted_bids AS (SELECT distinct
                         order_uuid AS order_uuid,
                         uuid       AS tender_uuid,
                         status     AS accepted_status,
                  FROM `indriver-e6e40.ods_new_order_rh_cdc.bid_global_strm`
                  WHERE true
                    AND TIMESTAMP_TRUNC(created_at, DAY) BETWEEN
                      TIMESTAMP_ADD(TIMESTAMP(PARSE_DATETIME('%Y-%m-%d', '{{date_range.start}}')), INTERVAL -1 DAY)
                      AND TIMESTAMP_ADD(TIMESTAMP(PARSE_DATETIME('%Y-%m-%d', '{{date_range.end}}')), INTERVAL 2 DAY)
                    AND status = 'BID_STATUS_ACCEPTED'
                    AND order_uuid IN (SELECT order_uuid FROM rider_data)),
                    
bid_data AS (SELECT distinct
                    CAST((ARRAY(SELECT DISTINCT city_id FROM rider_data LIMIT 1))[OFFSET(0)] AS INT64) AS city_id,
                    t.order_uuid                                                                       AS order_uuid,
                    price                                                                              AS price,
                    modified_at                                                                        AS modified_at_utc,
                    'bid_price'                                                                        AS price_type,
                    CAST(NULL AS INT64)                                                                AS price_highrate_value,
                    DENSE_RANK() OVER (ORDER BY t.order_uuid)                                          AS order_rn,
                    ROW_NUMBER() OVER (PARTITION BY t.order_uuid ORDER BY modified_at)                 AS tender_rn,
                    uuid                                                                               AS tender_uuid,
                    available_prices                                                                   AS available_prices,
                    contractor_id                                                                      AS contractor_id,
                    SAFE_CAST(SUBSTR(eta, 1, STRPOS(eta, 's') - 1) AS INT64)                           AS eta,
                    CAST(NULL AS INT64)                                                                AS duration_in_seconds,
                    accepted_status                                                                    AS accepted_status,
                    CAST(NULL AS STRING)                                                               AS order_type
             FROM `indriver-e6e40.ods_new_order_rh_cdc.bid_global_strm` t
             LEFT JOIN accepted_bids
                ON t.order_uuid = accepted_bids.order_uuid
                    AND t.uuid = accepted_bids.tender_uuid
             WHERE true
               AND TIMESTAMP_TRUNC(created_at, DAY) BETWEEN
                 TIMESTAMP_ADD(TIMESTAMP(PARSE_DATETIME('%Y-%m-%d', '{{date_range.start}}')), INTERVAL -1 DAY)
                 AND TIMESTAMP_ADD(TIMESTAMP(PARSE_DATETIME('%Y-%m-%d', '{{date_range.end}}')), INTERVAL 2 DAY)
               AND status = 'BID_STATUS_ACTIVE'
               AND t.order_uuid IN (SELECT order_uuid FROM rider_data)),

your_table AS (SELECT *
               FROM (SELECT *
                     FROM rider_data
                     UNION ALL
                     SELECT *
                     FROM bid_data)
               ORDER BY order_uuid, modified_at_utc),

pass_groups AS (SELECT order_uuid,
                       price,
                       modified_at_utc,
                       price_type,
                       SUM(CASE WHEN price_type = 'rider_price' THEN 1 ELSE 0 END)
                           OVER (PARTITION BY order_uuid ORDER BY modified_at_utc) AS pass_group,
                       price_highrate_value,
                       available_prices,
                       contractor_id,
                       tender_uuid,
                       city_id,
                       eta,
                       duration_in_seconds,
                       accepted_status
                FROM your_table),

full_data_table AS (SELECT city_id,
                           order_uuid,
                           price,
                           price_type,
                           modified_at_utc,
                           available_prices,
                           LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN price END IGNORE NULLS)
                                      OVER (PARTITION BY order_uuid, pass_group ORDER BY modified_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                               AS last_pass_price,
                           LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN price_highrate_value END IGNORE NULLS)
                                      OVER (PARTITION BY order_uuid ORDER BY modified_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                               AS price_highrate_value,
                           DATETIME_ADD(
                                   DATETIME_TRUNC(modified_at_utc, HOUR),
                                   INTERVAL
                                   CAST(FLOOR(EXTRACT(MINUTE FROM modified_at_utc) / {{freq_minutes}}) *
                                        {{freq_minutes}} AS INTEGER)
                                   MINUTE
                           )   AS utc_X_min,
                           contractor_id,
                           tender_uuid,
                           eta, 
                           duration_in_seconds,
                           accepted_status
                    FROM pass_groups),

options_assign AS (SELECT *,
                          CASE
                              WHEN price_type = 'rider_price' THEN NULL
                              ELSE
                                  COALESCE(
                                          (SELECT CASE
                                                      WHEN price = last_pass_price
                                                          THEN 'startprice'
                                                      WHEN price = available_prices[offset]
                                                          THEN 'option ' || CAST(offset + 1 AS STRING)
                                                      WHEN price < available_prices[SAFE_OFFSET(0)]
                                                          THEN 'option 1-'
                                                      WHEN price >
                                                           available_prices[SAFE_OFFSET(ARRAY_LENGTH(available_prices) - 1)]
                                                          THEN 'option ' || CAST(ARRAY_LENGTH(available_prices) AS STRING) || '+'
                                                      ELSE 'option ' || CAST(offset + 1 AS STRING) ||
                                                           (CASE WHEN price > available_prices[offset] THEN '+' ELSE '-' END)
                                                      END
                                           FROM UNNEST(available_prices) WITH OFFSET AS offset
                                           WHERE price = available_prices[offset]
                                              OR (price < available_prices[offset] AND
                                                  (offset = 0 OR price > available_prices[offset - 1]))
                                              OR (price > available_prices[offset] AND
                                                  (offset = ARRAY_LENGTH(available_prices) - 1 OR
                                                   price < available_prices[offset + 1]))
                                           LIMIT 1),
                                          'other'
                                  ) END AS option_number
                   FROM full_data_table),

result AS (SELECT DATETIME(TIMESTAMP(utc_X_min), tz.timezone)                                                        AS local_X_min,

                  COUNT(IF(option_number = 'startprice', t.order_uuid, NULL)) /
                  COUNT(t.order_uuid)                                                                                AS StartPriceBid,
                  COUNT(IF(option_number = 'option 1-', t.order_uuid, NULL)) /
                  COUNT(t.order_uuid)                                                                                AS OtherBid_0,
                  COUNT(IF(option_number = 'option 1', t.order_uuid, NULL)) /
                  COUNT(t.order_uuid)                                                                                AS ButtonBid_1,
                  COUNT(IF(option_number = 'option 1+', t.order_uuid, NULL)) /
                  COUNT(t.order_uuid)                                                                                AS OtherBid_1,
                  COUNT(IF(option_number = 'option 2', t.order_uuid, NULL)) /
                  COUNT(t.order_uuid)                                                                                AS ButtonBid_2,
                  COUNT(IF(option_number = 'option 2+', t.order_uuid, NULL)) /
                  COUNT(t.order_uuid)                                                                                AS OtherBid_2,
                  COUNT(IF(option_number = 'option 3', t.order_uuid, NULL)) /
                  COUNT(t.order_uuid)                                                                                AS ButtonBid_3,
                  COUNT(IF(option_number = 'option 3+', t.order_uuid, NULL)) /
                  COUNT(t.order_uuid)                                                                                AS OtherBid_3,
                  COUNT(IF(option_number = 'option 4', t.order_uuid, NULL)) /
                  COUNT(t.order_uuid)                                                                                AS ButtonBid_4,
                  COUNT(IF(option_number = 'option 4+', t.order_uuid, NULL)) /
                  COUNT(t.order_uuid)                                                                                AS OtherBid_4,
                  COUNT(IF(option_number = 'other', t.order_uuid, NULL)) /
                  COUNT(t.order_uuid)                                                                                AS opt_other_share,

                  SAFE_DIVIDE(COUNT(CASE
                                        WHEN option_number = 'startprice' AND accepted_status = 'BID_STATUS_ACCEPTED'
                                            THEN t.order_uuid
                                        ELSE NULL END),
                              COUNT(IF(option_number = 'startprice', t.order_uuid, NULL)))                           AS bid2acc_StartPriceBid,
                  SAFE_DIVIDE(COUNT(CASE
                                        WHEN option_number = 'option 1-' AND accepted_status = 'BID_STATUS_ACCEPTED'
                                            THEN t.order_uuid
                                        ELSE NULL END),
                              COUNT(IF(option_number = 'option 1-', t.order_uuid, NULL)))                            AS bid2acc_OtherBid_0,
                  SAFE_DIVIDE(COUNT(CASE
                                        WHEN option_number = 'option 1' AND accepted_status = 'BID_STATUS_ACCEPTED'
                                            THEN t.order_uuid
                                        ELSE NULL END),
                              COUNT(IF(option_number = 'option 1', t.order_uuid, NULL)))                             AS bid2acc_ButtonBid_1,
                  SAFE_DIVIDE(COUNT(CASE
                                        WHEN option_number = 'option 1+' AND accepted_status = 'BID_STATUS_ACCEPTED'
                                            THEN t.order_uuid
                                        ELSE NULL END),
                              COUNT(IF(option_number = 'option 1+', t.order_uuid, NULL)))                            AS bid2acc_OtherBid_1,
                  SAFE_DIVIDE(COUNT(CASE
                                        WHEN option_number = 'option 2' AND accepted_status = 'BID_STATUS_ACCEPTED'
                                            THEN t.order_uuid
                                        ELSE NULL END),
                              COUNT(IF(option_number = 'option 2', t.order_uuid, NULL)))                             AS bid2acc_ButtonBid_2,
                  SAFE_DIVIDE(COUNT(CASE
                                        WHEN option_number = 'option 2+' AND accepted_status = 'BID_STATUS_ACCEPTED'
                                            THEN t.order_uuid
                                        ELSE NULL END),
                              COUNT(IF(option_number = 'option 2+', t.order_uuid, NULL)))                            AS bid2acc_OtherBid_2,
                  SAFE_DIVIDE(COUNT(CASE
                                        WHEN option_number = 'option 3' AND accepted_status = 'BID_STATUS_ACCEPTED'
                                            THEN t.order_uuid
                                        ELSE NULL END),
                              COUNT(IF(option_number = 'option 3', t.order_uuid, NULL)))                             AS bid2acc_ButtonBid_3,
                  SAFE_DIVIDE(COUNT(CASE
                                        WHEN option_number = 'option 3+' AND accepted_status = 'BID_STATUS_ACCEPTED'
                                            THEN t.order_uuid
                                        ELSE NULL END),
                              COUNT(IF(option_number = 'option 3+', t.order_uuid, NULL)))                            AS bid2acc_OtherBid_3,
                              
                  COUNT(IF(price / price_highrate_value < 0.8, t.order_uuid, NULL)) /
                  COUNT(t.order_uuid)                                                                                AS bid2rec_to08,
                  COUNT(IF(price / price_highrate_value >= 0.8 AND price / price_highrate_value < 0.9, t.order_uuid,
                           NULL)) /
                  COUNT(t.order_uuid)                                                                                AS bid2rec_08to09,
                  COUNT(IF(price / price_highrate_value >= 0.9 AND price / price_highrate_value < 1.0, t.order_uuid,
                           NULL)) /
                  COUNT(t.order_uuid)                                                                                AS bid2rec_09to1,
                  COUNT(IF(price / price_highrate_value = 1.0, t.order_uuid, NULL)) /
                  COUNT(t.order_uuid)                                                                                AS bid2rec_1,
                  COUNT(IF(price / price_highrate_value > 1.0 AND price / price_highrate_value < 1.1, t.order_uuid,
                           NULL)) /
                  COUNT(t.order_uuid)                                                                                AS bid2rec_1to11,
                  COUNT(IF(price / price_highrate_value >= 1.1 AND price / price_highrate_value < 1.2, t.order_uuid,
                           NULL)) /
                  COUNT(t.order_uuid)                                                                                AS bid2rec_11to12,
                  COUNT(IF(price / price_highrate_value >= 1.2 AND price / price_highrate_value < 1.3, t.order_uuid,
                           NULL)) /
                  COUNT(t.order_uuid)                                                                                AS bid2rec_12to13,
                  COUNT(IF(price / price_highrate_value >= 1.3 AND price / price_highrate_value < 1.4, t.order_uuid,
                           NULL)) /
                  COUNT(t.order_uuid)                                                                                AS bid2rec_13to14,
                  COUNT(IF(price / price_highrate_value >= 1.4 AND price / price_highrate_value < 1.5, t.order_uuid,
                           NULL)) /
                  COUNT(t.order_uuid)                                                                                AS bid2rec_14to15,
                  COUNT(IF(price / price_highrate_value >= 1.5, t.order_uuid,NULL)) /
                  COUNT(t.order_uuid)                                                                                AS bid2rec_15to,
                  
                  SAFE_DIVIDE(COUNT(CASE
                                        WHEN price / price_highrate_value < 0.8 
                                        AND accepted_status = 'BID_STATUS_ACCEPTED'
                                            THEN t.order_uuid
                                        ELSE NULL END),
                              COUNT(IF(price / price_highrate_value < 0.8, t.order_uuid, NULL)))                    AS Bid_to08,
                  SAFE_DIVIDE(COUNT(CASE
                                        WHEN price / price_highrate_value >= 0.8 
                                        AND price / price_highrate_value < 0.9 
                                        AND accepted_status = 'BID_STATUS_ACCEPTED'
                                            THEN t.order_uuid
                                        ELSE NULL END),
                              COUNT(IF(price / price_highrate_value >= 0.8 
                                       AND price / price_highrate_value < 0.9, t.order_uuid, NULL)))                 AS Bid_08to09,
                  SAFE_DIVIDE(COUNT(CASE
                                        WHEN price / price_highrate_value >= 0.9 
                                        AND price / price_highrate_value < 1.0 
                                        AND accepted_status = 'BID_STATUS_ACCEPTED'
                                            THEN t.order_uuid
                                        ELSE NULL END),
                              COUNT(IF(price / price_highrate_value >= 0.9 
                                       AND price / price_highrate_value < 1.0, t.order_uuid, NULL)))                 AS Bid_09to1,
                  SAFE_DIVIDE(COUNT(CASE
                                        WHEN price / price_highrate_value = 1.0 
                                        AND accepted_status = 'BID_STATUS_ACCEPTED'
                                            THEN t.order_uuid
                                        ELSE NULL END),
                              COUNT(IF(price / price_highrate_value = 1.0 , t.order_uuid, NULL)))                    AS RecPriceBid,
                  SAFE_DIVIDE(COUNT(CASE
                                        WHEN price / price_highrate_value > 1.0 
                                        AND price / price_highrate_value < 1.1 
                                        AND accepted_status = 'BID_STATUS_ACCEPTED'
                                            THEN t.order_uuid
                                        ELSE NULL END),
                              COUNT(IF(price / price_highrate_value > 1.0 
                                       AND price / price_highrate_value < 1.1, t.order_uuid, NULL)))                 AS Bid_1to11,
                  SAFE_DIVIDE(COUNT(CASE
                                        WHEN price / price_highrate_value >= 1.1 
                                        AND price / price_highrate_value < 1.2 
                                        AND accepted_status = 'BID_STATUS_ACCEPTED'
                                            THEN t.order_uuid
                                        ELSE NULL END),
                              COUNT(IF(price / price_highrate_value >= 1.1 
                                       AND price / price_highrate_value < 1.2, t.order_uuid, NULL)))                 AS Bid_11to12,
                  SAFE_DIVIDE(COUNT(CASE
                                        WHEN price / price_highrate_value >= 1.2 
                                        AND price / price_highrate_value < 1.3 
                                        AND accepted_status = 'BID_STATUS_ACCEPTED'
                                            THEN t.order_uuid
                                        ELSE NULL END),
                              COUNT(IF(price / price_highrate_value >= 1.2 
                                       AND price / price_highrate_value < 1.3, t.order_uuid, NULL)))                 AS Bid_12to13,
                  SAFE_DIVIDE(COUNT(CASE
                                        WHEN price / price_highrate_value >= 1.3 
                                        AND price / price_highrate_value < 1.4 
                                        AND accepted_status = 'BID_STATUS_ACCEPTED'
                                            THEN t.order_uuid
                                        ELSE NULL END),
                              COUNT(IF(price / price_highrate_value >= 1.3 
                                       AND price / price_highrate_value < 1.4, t.order_uuid, NULL)))                 AS Bid_13to14,
                  SAFE_DIVIDE(COUNT(CASE
                                        WHEN price / price_highrate_value >= 1.4 
                                        AND price / price_highrate_value < 1.5 
                                        AND accepted_status = 'BID_STATUS_ACCEPTED'
                                            THEN t.order_uuid
                                        ELSE NULL END),
                              COUNT(IF(price / price_highrate_value >= 1.4 
                                       AND price / price_highrate_value < 1.5, t.order_uuid, NULL)))                 AS Bid_14to15,
                  SAFE_DIVIDE(COUNT(CASE
                                        WHEN price / price_highrate_value >= 1.5 
                                        AND accepted_status = 'BID_STATUS_ACCEPTED'
                                            THEN t.order_uuid
                                        ELSE NULL END),
                              COUNT(IF(price / price_highrate_value >= 1.5, t.order_uuid, NULL)))                 AS Bid_15to
                
           FROM options_assign AS t
                    LEFT JOIN timezone_data tz
                              ON t.city_id = tz.city_id
           WHERE price_type = 'bid_price'
           GROUP BY local_X_min)

SELECT *
FROM result
WHERE DATETIME_TRUNC(local_X_min, DAY) BETWEEN
          DATETIME_TRUNC(PARSE_DATETIME('%Y-%m-%d', '{{date_range.start}}'), DAY)
          AND DATETIME_TRUNC(PARSE_DATETIME('%Y-%m-%d', '{{date_range.end}}'), DAY)
order by local_X_min;


