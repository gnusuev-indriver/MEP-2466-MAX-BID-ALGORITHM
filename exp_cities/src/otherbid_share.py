import pandas as pd
from google.cloud import bigquery
from typing import List

# Инициализация клиента BigQuery (аналогично download.py)
# Предполагается, что проект 'analytics-dev-333113' используется для операций клиента
client = bigquery.Client(project='analytics-dev-333113')

def calculate_average_option_3_plus_share(
    city_ids: List[int],
    order_types: List[str],
    start_date: str,
    end_date: str,
    print_query: bool = False
) -> pd.DataFrame:
    """
    Рассчитывает среднюю долю бидов 'option 3+' в разрезе городов и типов заказов
    для Google BigQuery и возвращает результат в виде Pandas DataFrame.

    Args:
        city_ids (List[int]): Список ID городов, например: [123, 456].
        order_types (List[str]): Список типов заказов, например: ["COMFORT", "ECONOMY"].
        start_date (str): Дата начала периода в формате 'YYYY-MM-DD'.
        end_date (str): Дата окончания периода в формате 'YYYY-MM-DD'.
        print_query (bool): Если True, напечатать сгенерированный SQL-запрос.

    Returns:
        pd.DataFrame: DataFrame с результатами: city_id, order_type, avg_share_option_3_plus_bids.
    """
    
    # Параметры city_ids и order_types будут напрямую вставлены в UNNEST()
    # Даты start_date и end_date будут вставлены как строки в PARSE_DATE()

    sql_query = f"""
WITH
timezone_data AS (
    SELECT 
        city_id, 
        timezone
    FROM `indriver-e6e40.emart.incity_detail`
    WHERE 
        city_id IN UNNEST({city_ids})
        AND created_date_order_part BETWEEN DATE_SUB(PARSE_DATE('%Y-%m-%d', '{start_date}'), INTERVAL 7 DAY) 
                                      AND PARSE_DATE('%Y-%m-%d', '{end_date}') 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city_id ORDER BY created_date_order_part DESC) = 1
),

rider_data AS (
    SELECT 
        t1.city_id,
        t1.uuid AS order_uuid,
        payment_price_value AS price,
        t1.modified_at AS modified_at_utc,
        'rider_price' AS price_type,
        price_highrate_value,
        CAST(NULL AS INT64) AS order_rn,
        CAST(NULL AS INT64) AS tender_rn,
        CAST(NULL AS STRING) AS tender_uuid,
        CAST(NULL AS ARRAY<INT64>) AS available_prices,
        CAST(NULL AS INT64) AS contractor_id,
        CAST(NULL AS INT64) AS eta,
        t2.duration_in_seconds,
        CAST(NULL AS STRING) AS accepted_status,
        type_name AS order_type
    FROM `indriver-e6e40.ods_new_order_rh_cdc.order_global_strm` t1
    LEFT JOIN timezone_data tz ON t1.city_id = tz.city_id
    LEFT JOIN (
        SELECT 
            order_uuid,
            duration_in_seconds
        FROM `indriver-e6e40.emart.incity_detail`
        WHERE 
            city_id IN UNNEST({city_ids})
            AND created_date_order_part BETWEEN PARSE_DATE('%Y-%m-%d', '{start_date}') AND PARSE_DATE('%Y-%m-%d', '{end_date}')
    ) t2 ON t1.uuid = t2.order_uuid  
    WHERE 
        TRUE
        AND t1.city_id IN UNNEST({city_ids})
        AND type_name IN UNNEST({order_types})
        AND TIMESTAMP_TRUNC(t1.created_at, DAY) BETWEEN
            TIMESTAMP_ADD(TIMESTAMP(PARSE_DATE('%Y-%m-%d', '{start_date}')), INTERVAL -1 DAY)
            AND TIMESTAMP_ADD(TIMESTAMP(PARSE_DATE('%Y-%m-%d', '{end_date}')), INTERVAL 2 DAY)
        AND t1.created_at BETWEEN 
            TIMESTAMP(PARSE_DATE('%Y-%m-%d', '{start_date}'), tz.timezone)
            AND TIMESTAMP_ADD(TIMESTAMP(PARSE_DATE('%Y-%m-%d', '{end_date}'), tz.timezone), INTERVAL 1 DAY) 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY t1.uuid, payment_price_value ORDER BY t1.modified_at) = 1
),

accepted_bids AS (
    SELECT DISTINCT
        order_uuid,
        uuid AS tender_uuid,
        status AS accepted_status
    FROM `indriver-e6e40.ods_new_order_rh_cdc.bid_global_strm`
    WHERE 
        TRUE
        AND TIMESTAMP_TRUNC(created_at, DAY) BETWEEN
            TIMESTAMP_ADD(TIMESTAMP(PARSE_DATE('%Y-%m-%d', '{start_date}')), INTERVAL -1 DAY)
            AND TIMESTAMP_ADD(TIMESTAMP(PARSE_DATE('%Y-%m-%d', '{end_date}')), INTERVAL 2 DAY)
        AND status = 'BID_STATUS_ACCEPTED'
        AND order_uuid IN (SELECT order_uuid FROM rider_data)
),
                    
bid_data AS (
    SELECT DISTINCT
        rd.city_id,
        t.order_uuid,
        t.price,
        t.modified_at AS modified_at_utc,
        'bid_price' AS price_type,
        CAST(NULL AS INT64) AS price_highrate_value,
        DENSE_RANK() OVER (ORDER BY t.order_uuid) AS order_rn,
        ROW_NUMBER() OVER (PARTITION BY t.order_uuid ORDER BY t.modified_at) AS tender_rn,
        t.uuid AS tender_uuid,
        t.available_prices,
        t.contractor_id,
        SAFE_CAST(SUBSTR(t.eta, 1, STRPOS(t.eta, 's') - 1) AS INT64) AS eta,
        CAST(NULL AS INT64) AS duration_in_seconds,
        ab.accepted_status,
        rd.order_type
    FROM `indriver-e6e40.ods_new_order_rh_cdc.bid_global_strm` t
    JOIN (SELECT DISTINCT order_uuid, city_id, order_type FROM rider_data) rd 
        ON t.order_uuid = rd.order_uuid
    LEFT JOIN timezone_data tz ON rd.city_id = tz.city_id
    LEFT JOIN accepted_bids ab 
        ON t.order_uuid = ab.order_uuid AND t.uuid = ab.tender_uuid
    WHERE 
        TRUE
        AND t.status = 'BID_STATUS_ACTIVE'
        AND TIMESTAMP_TRUNC(t.created_at, DAY) BETWEEN 
            TIMESTAMP_ADD(TIMESTAMP(PARSE_DATE('%Y-%m-%d', '{start_date}')), INTERVAL -1 DAY)
            AND TIMESTAMP_ADD(TIMESTAMP(PARSE_DATE('%Y-%m-%d', '{end_date}')), INTERVAL 2 DAY)
        AND t.created_at BETWEEN 
             TIMESTAMP(PARSE_DATE('%Y-%m-%d', '{start_date}'), tz.timezone)
             AND TIMESTAMP_ADD(TIMESTAMP(PARSE_DATE('%Y-%m-%d', '{end_date}'), tz.timezone), INTERVAL 1 DAY)
        AND t.order_uuid IN (SELECT order_uuid FROM rider_data)
),

your_table AS (
    SELECT
        city_id, order_uuid, price, modified_at_utc, price_type, price_highrate_value,
        order_rn, tender_rn, tender_uuid, available_prices, contractor_id, eta,
        duration_in_seconds, accepted_status, order_type
    FROM rider_data
    UNION ALL
    SELECT
        city_id, order_uuid, price, modified_at_utc, price_type, price_highrate_value,
        order_rn, tender_rn, tender_uuid, available_prices, contractor_id, eta,
        duration_in_seconds, accepted_status, order_type
    FROM bid_data
),

pass_groups AS (
    SELECT 
        order_uuid, price, modified_at_utc, price_type,
        SUM(CASE WHEN price_type = 'rider_price' THEN 1 ELSE 0 END)
            OVER (PARTITION BY order_uuid ORDER BY modified_at_utc) AS pass_group,
        price_highrate_value, available_prices, contractor_id, tender_uuid, city_id,
        eta, duration_in_seconds, accepted_status, order_type
    FROM your_table
),

full_data_table AS (
    SELECT 
        city_id, order_uuid, price, price_type, modified_at_utc, available_prices,
        LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN price END IGNORE NULLS)
            OVER (PARTITION BY order_uuid, pass_group ORDER BY modified_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            AS last_pass_price,
        LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN price_highrate_value END IGNORE NULLS)
            OVER (PARTITION BY order_uuid ORDER BY modified_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            AS price_highrate_value,
        contractor_id, tender_uuid, eta, duration_in_seconds, accepted_status, order_type
    FROM pass_groups
),

options_assign AS (
    SELECT *,
           CASE
               WHEN price_type = 'rider_price' THEN NULL
               ELSE
                   COALESCE(
                       (SELECT CASE
                                   WHEN price = last_pass_price THEN 'startprice'
                                   WHEN price = available_prices[offset_idx] THEN 'option ' || CAST(offset_idx + 1 AS STRING)
                                   WHEN price < available_prices[SAFE_OFFSET(0)] THEN 'option 1-'
                                   WHEN price > available_prices[SAFE_OFFSET(ARRAY_LENGTH(available_prices) - 1)] THEN
                                       'option ' || CAST(ARRAY_LENGTH(available_prices) AS STRING) || '+'
                                   ELSE 'option ' || CAST(offset_idx + 1 AS STRING) ||
                                        (CASE WHEN price > available_prices[offset_idx] THEN '+' ELSE '-' END)
                                   END
                        FROM UNNEST(GENERATE_ARRAY(0, IFNULL(ARRAY_LENGTH(available_prices),0) - 1)) AS offset_idx
                        WHERE (price = available_prices[offset_idx])
                           OR (price < available_prices[offset_idx] AND (offset_idx = 0 OR price > available_prices[SAFE_OFFSET(offset_idx - 1)]))
                           OR (price > available_prices[offset_idx] AND (offset_idx = ARRAY_LENGTH(available_prices) - 1 OR price < available_prices[SAFE_OFFSET(offset_idx + 1)]))
                        LIMIT 1
                       ),
                       'other'
                   ) END AS option_number
    FROM full_data_table
),

bids_with_options AS (
    SELECT
        city_id,
        order_uuid,
        order_type,
        option_number,
        tender_uuid 
    FROM options_assign
    WHERE price_type = 'bid_price' 
      AND tender_uuid IS NOT NULL 
),

order_level_shares AS (
    SELECT
        city_id,
        order_uuid,
        order_type,
        COUNTIF(option_number = 'option 3+') * 1.0 / COUNT(tender_uuid) AS share_option_3_plus
    FROM bids_with_options
    GROUP BY city_id, order_uuid, order_type
    HAVING COUNT(tender_uuid) > 0 
),

final_result AS (
    SELECT
        city_id,
        order_type,
        AVG(share_option_3_plus) AS avg_share_option_3_plus_bids
    FROM order_level_shares
    GROUP BY city_id, order_type
)

SELECT *
FROM final_result
ORDER BY city_id, order_type;
"""

    if print_query:
        print("--- Generated SQL Query ---")
        print(sql_query)
        print("-------------------------")

    # Выполнение запроса и получение результатов в DataFrame
    query_job = client.query(sql_query)
    results_df = query_job.result().to_dataframe()

    return results_df

