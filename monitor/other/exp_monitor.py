import pandas as pd
import numpy as np

def get_monitor_data(start_date = '2025-02-01',
                      stop_date = '2025-02-04',
                      city_type_conditions = ()):
        
        df_og = pd.read_gbq(f'''
                        WITH rider_data AS (SELECT t1.city_id                     AS city_id,
                            timezone                    AS timezone,
                            type_name                   AS type_name,
                            uuid                        AS order_uuid,
                            CAST(NULL AS STRING)        AS tender_uuid,
                            'rider_price'               AS price_type,
                            payment_price_value         AS price,
                            CAST(NULL AS ARRAY <INT64>) AS available_prices,
                            price_highrate_value        AS price_highrate_value,
                            --accepted_tender_uuid        AS accepted_tender_uuid,
                            --AtoB_seconds                AS AtoB_seconds,
                            CAST(NULL AS INT64)         AS eta,
                            modified_at                 AS modified_at_utc,
                            --order_done                  AS order_done,
                            multiplier                  AS multiplier,
                            CAST(NULL AS STRING) as bidding_algorithm_name
                        FROM `indriver-e6e40.ods_new_order_rh_cdc.order_global_strm` t1
                                LEFT JOIN (SELECT city_id,
                                                MAX(timezone)                                                            AS timezone,
                                                --MAX(CASE WHEN driverdone_timestamp IS NOT NULL THEN tender_uuid END)     AS accepted_tender_uuid,
                                                --MAX(CASE WHEN driverdone_timestamp IS NOT NULL THEN true ELSE false END) AS order_done,
                                                --MAX(duration_in_seconds)                                                 AS AtoB_seconds
                                            FROM `indriver-e6e40.emart.incity_detail`
                                            WHERE true
                                            AND created_date_order_part BETWEEN
                                                DATE_SUB('{start_date}', INTERVAL 2 DAY)
                                                AND DATE_ADD('{stop_date}', INTERVAL 2 DAY)
                                            GROUP BY city_id) t2
                                        ON t1.city_id = t2.city_id
                        WHERE true
                        --AND order_uuid IS NOT NULL
                        AND status = 'ORDER_STATUS_ACTIVE'
                        --AND AtoB_seconds > 0
                        AND {city_type_conditions}
                        AND DATE(created_at) BETWEEN
                            DATE_SUB('{start_date}', INTERVAL 2 DAY)
                            AND DATE_ADD('{stop_date}', INTERVAL 2 DAY)
                        QUALIFY ROW_NUMBER() OVER (PARTITION BY uuid, payment_price_value ORDER BY modified_at) = 1),

        bid_data AS (SELECT DISTINCT CAST(NULL AS INT64)                                      AS city_id,
                                    CAST(NULL AS STRING)                                     AS timezone,
                                    CAST(NULL AS STRING)                                     AS type_name,
                                    order_uuid                                               AS order_uuid,
                                    uuid                                                     AS tender_uuid,
                                    'bid_price'                                              AS price_type,
                                    price                                                    AS price,
                                    available_prices                                         AS available_prices,
                                    CAST(NULL AS INT64)                                      AS price_highrate_value,
                                    --CAST(NULL AS STRING)                                     AS accepted_tender_uuid,
                                    --CAST(NULL AS INT64)                                      AS AtoB_seconds,
                                    SAFE_CAST(SUBSTR(eta, 1, STRPOS(eta, 's') - 1) AS INT64) AS eta,
                                    modified_at                                              AS modified_at_utc,
                                    --CAST(NULL AS BOOL)                                       AS order_done,
                                    CAST(NULL AS INT64)                                      AS multiplier,
                                    bidding_algorithm_name as bidding_algorithm_name
                    FROM `indriver-e6e40.ods_new_order_rh_cdc.bid_global_strm`
                    WHERE true
                        AND order_uuid IS NOT NULL
                        AND uuid IS NOT NULL
                        AND order_uuid IN (SELECT order_uuid FROM rider_data)
                        AND status = 'BID_STATUS_ACTIVE'
                        AND DATE(created_at) BETWEEN
                        DATE_SUB('{start_date}', INTERVAL 2 DAY)
                        AND DATE_ADD('{stop_date}', INTERVAL 2 DAY)),

        my_strm AS (SELECT city_id,
                            timezone,
                            type_name,
                            order_uuid,
                            tender_uuid,
                            price_type,
                            price,
                            available_prices,
                            price_highrate_value,
                            --accepted_tender_uuid,
                            --AtoB_seconds,
                            eta,
                            modified_at_utc,
                            --order_done,
                            multiplier,
                            bidding_algorithm_name
                    FROM (SELECT *
                        FROM rider_data
                        UNION ALL
                        SELECT *
                        FROM bid_data)
                    ORDER BY order_uuid, modified_at_utc),

        my_strm_full AS (SELECT LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN city_id END IGNORE NULLS)
                                            OVER w                                                           AS city_id,
                                LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN timezone END IGNORE NULLS)
                                            OVER w                                                           AS timezone,
                                LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN type_name END IGNORE NULLS)
                                            OVER w                                                           AS type_name,
                                order_uuid,
                                tender_uuid,
                                price_type,
                                price / coalesce(multiplier, 100)                                           AS price,
                                ARRAY(SELECT available_price / COALESCE(multiplier, 100)
                                    FROM UNNEST(available_prices) AS available_price)                       AS available_prices,
                                ARRAY_REVERSE(available_prices)[SAFE_OFFSET(0)] / COALESCE(multiplier, 100) AS last_step,
                                LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN price END IGNORE
                                            NULLS)
                                            OVER w /
                                coalesce(multiplier, 100)                                                   AS start_price_value,
                                LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN price_highrate_value END IGNORE
                                            NULLS)
                                            OVER w /
                                coalesce(multiplier, 100)                                                   AS price_highrate_value,
                                --LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN accepted_tender_uuid END IGNORE
                                --        NULLS)
                                --        OVER w                                                           AS accepted_tender_uuid,
                                --LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN AtoB_seconds END IGNORE
                                --        NULLS)
                                --        OVER w                                                           AS AtoB_seconds,
                                eta,
                                modified_at_utc,
                                --LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN order_done END IGNORE
                                --        NULLS) OVER w                                                    AS order_done,

                                LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN multiplier END IGNORE
                                            NULLS) OVER w                                                    AS multiplier,
                                bidding_algorithm_name
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
                                price,
                                start_price_value,
                                price_highrate_value,
                                --AtoB_seconds,
                                eta,
                                --CAST(FLOOR(AtoB_seconds / 60) * 60 AS INT64)                        AS AtoB_seconds_bin,
                                CAST(FLOOR(eta / 60) * 60 AS INT64)                                 AS eta_bin,
                                available_prices,
                                bidding_algorithm_name,
                                --accepted_tender_uuid,
                                --order_done,
                                modified_at_utc as modified_at_utc,
                                DATETIME(TIMESTAMP(modified_at_utc), timezone)                      AS modified_at_local,
                                multiplier
                            FROM my_strm_full
                            WHERE true
                            AND eta IS NOT NULL
                            AND available_prices IS NOT NULL
                            AND price_highrate_value IS NOT NULL
                            AND price_type = 'bid_price'
                            AND DATE(DATETIME(TIMESTAMP(modified_at_utc), timezone)) BETWEEN DATE('{start_date}') AND DATE('{stop_date}')
                            ORDER BY modified_at_local desc)

        SELECT  city_id,
                type_name,
                modified_at_utc,
                modified_at_local, 
                price, 
                start_price_value, 
                price_highrate_value, 
                --AtoB_seconds, 
                eta,
                available_prices, 
                bidding_algorithm_name,
                -- max_bid, 
                -- simulated_bids,
                multiplier,
                CASE WHEN bidding_algorithm_name = 'bid_mph' THEN TRUE ELSE FALSE END AS new_bids_bool
        FROM options_assign;
        ''')

        print(start_date)
        print(stop_date)
        # df_og['distinct_simulated_bids_count'] = df_og['simulated_bids'].apply(lambda x: len(np.unique(x)))

        # Calculate total rows per group
        total_counts = df_og.groupby(['city_id', 'type_name'])['new_bids_bool'].count().rename('total_count')
        # Calculate count of True values per group
        true_counts = df_og.groupby(['city_id', 'type_name'])['new_bids_bool'].sum().rename('true_count')
        # Compute the share
        share_df = (true_counts / total_counts).rename('new_bids_share').reset_index()

        # Compute the original frequency table
        # freq_df = (df_og.groupby(['city_id', 'type_name'])['distinct_simulated_bids_count']
        #                 .value_counts()
        #                 .rename('frequency')
        #                 .reset_index())
        result=[]
        # Merge the share column into the frequency table
        # result = freq_df.merge(share_df, on=['city_id', 'type_name'], how='left')

        # # Сортируем результаты
        # result = result.sort_values(['city_id', 'type_name', 'distinct_simulated_bids_count'], ascending=[True, True, False])

        # # Опционально: добавляем процентное соотношение внутри каждой группы
        # result['percentage'] = (result.groupby(['city_id', 'type_name'])['frequency']
        #                                         .transform(lambda x: x / x.sum() * 100))

        # # Округляем процентные значения
        # result['percentage'] = result['percentage'].round(2)

        # # Переупорядочиваем столбцы
        # result = result[['city_id', 'type_name', 'new_bids_share', 'distinct_simulated_bids_count', 'frequency', 'percentage']]

        return [df_og, share_df]





# -- SAFE_DIVIDE(SAFE_DIVIDE(price, (AtoB_seconds + eta)),
#                                 --            SAFE_DIVIDE(GREATEST(price_highrate_value, start_price_value),
#                                 --                        (AtoB_seconds + {pickup_eta_minutes_case} * 60))) AS ratio,
#                                 -- last_step,
#                                 -- CASE
#                                 --     WHEN last_step <= SAFE_DIVIDE((1 + {alpha_case}) *
#                                 --                                             GREATEST(price_highrate_value, start_price_value) *
#                                 --                                             (AtoB_seconds + eta),
#                                 --                                             (AtoB_seconds + {pickup_eta_minutes_case} * 60)) THEN false
#                                 --                                    ELSE
#                                 --                                        true
#                                 --                                    END                                                             AS new_bids_bool,
#                                 -- SAFE_DIVIDE((1 + {alpha_case}) *
#                                 --            GREATEST(price_highrate_value, start_price_value) *
#                                 --            (AtoB_seconds + eta),
#                                 --            (AtoB_seconds + {pickup_eta_minutes_case} * 60))              AS max_bid,
#                                 --    ARRAY(
#                                 --        SELECT start_price_value + SAFE_DIVIDE(n, ARRAY_LENGTH(available_prices)) *
#                                 --               (SAFE_DIVIDE((1 + {alpha_case}) *
#                                 --                            GREATEST(price_highrate_value, start_price_value) *
#                                 --                            (AtoB_seconds + eta),
#                                 --                            (AtoB_seconds + {pickup_eta_minutes_case} * 60)) -
#                                 --                    start_price_value)
#                                 --                FROM UNNEST(GENERATE_ARRAY(1, ARRAY_LENGTH(available_prices))) AS n
#                                 --                                )                                                                   AS new_bids,
#                                 --                                CAST([{step1_case}, {step2_case}] AS ARRAY <INT64>)                             AS rounds,
#                                 -- CASE
#                                 --     WHEN last_step <= SAFE_DIVIDE((1 + {alpha_case}) *
#                                 --                                             GREATEST(price_highrate_value, start_price_value) *
#                                 --                                             (AtoB_seconds + eta),
#                                 --                                             (AtoB_seconds + {pickup_eta_minutes_case} * 60))
#                                 --        THEN available_prices
#                                 --                                    ELSE ARRAY(SELECT
#                                 --                                                CASE
#                                 --                                                  WHEN {step2_case} >= 0
#                                 --                                                      THEN CASE
#                                 --                                                        WHEN price <=
#                                 --                                                                 (FLOOR(price / {step2_case}) *
#                                 --                                                                  {step2_case} +
#                                 --                                                                  CEIL(price / {step2_case}) *
#                                 --                                                                  {step2_case}) / 2
#                                 --                                                                 THEN
#                                 --                                                                 CEIL(price / {step1_case}) *
#                                 --                                                                 {step1_case}
#                                 --                                                               ELSE
#                                 --                                                                   CEIL(price / {step2_case}) *
#                                 --                                                                   {step2_case} END
#                                 --                                                  ELSE CEIL(price / {step1_case}) *
#                                 --                                                       {step1_case} END
#                                 --                FROM UNNEST(ARRAY(
#                                 --                                                    SELECT
#                                 --                                                    start_price_value + SAFE_DIVIDE(n, ARRAY_LENGTH(available_prices)) *
#                                 --                                                                        (SAFE_DIVIDE((1 + {alpha_case}) *
#                                 --                                                                                        GREATEST(price_highrate_value, start_price_value) *
#                                 --                                                                                        (AtoB_seconds + eta),
#                                 --                                                                                        (AtoB_seconds + {pickup_eta_minutes_case} * 60)) -
#                                 --                                                                            start_price_value)
#                                 --                                                    FROM UNNEST(GENERATE_ARRAY(1, ARRAY_LENGTH(available_prices))) AS n
#                                 --                                                            )) AS price) END                         as simulated_bids,