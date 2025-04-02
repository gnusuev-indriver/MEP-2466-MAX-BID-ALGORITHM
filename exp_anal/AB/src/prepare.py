import warnings
warnings.filterwarnings("ignore")

import h3
import numpy as np
import pandas as pd


def get_hex(df, hex_size):
    df[f'hex_from_calc_{hex_size}'] = [h3.geo_to_h3(x, y, hex_size) for x, y in zip(df['fromlatitude'], df['fromlongitude'])]
    return df


def convert_ts_to_timestamp(df):
    try:
        df['ts'] = df['ts'].dt.to_timestamp()
    except:
        df['ts'] = df['ts']
    return df


def get_ts(df, date_column_name, by_time_resolution):
    df['ts'] = df[date_column_name].dt.floor(by_time_resolution)
    df['ts'] = pd.DatetimeIndex(df['ts']).to_period(by_time_resolution)
    df = convert_ts_to_timestamp(df)
    return df


def prepare_recprice_data(df):
    df['group_name'] = df['recprice_group_name']
#     df = df[
#         ~(df['log_duration_in_min'].isnull()) &
#         ~(df['log_distance_in_km'].isnull())
#     ]
#     duration_min = np.quantile(df['log_duration_in_min'], q=0.01)
#     duration_max = np.quantile(df['log_duration_in_min'], q=0.99)
#     distance_min = np.quantile(df['log_distance_in_km'], q=0.01)
#     distance_max = np.quantile(df['log_distance_in_km'], q=0.99)
#     df = df[(df['log_duration_in_min'] > duration_min) & (df['log_duration_in_min'] < duration_max)]
#     df = df[(df['log_distance_in_km'] > distance_min) & (df['log_distance_in_km'] < distance_max)]
    df['utc_dt'] = df['utc_recprice_dttm'].dt.date
    df['utc_hour'] = df['utc_recprice_dttm'].dt.hour
    df['utc_weekday'] = df['utc_recprice_dttm'].dt.weekday
    df['local_dt'] = df['local_recprice_dttm'].dt.date
    df['local_hour'] = df['local_recprice_dttm'].dt.hour
    df['local_weekday'] = df['local_recprice_dttm'].dt.weekday
    df = get_ts(df, date_column_name='local_recprice_dttm', by_time_resolution='30min')
    df['time'] = df['ts'].dt.time
    # df = get_hex(df, hex_size=7)
    df.reset_index(drop=True, inplace=True)
    return df


def prepare_order_data(df):
    df['group_name'] = df['order_group_name']
#     df = df[
#         ~(df['duration_in_min'].isnull()) &
#         ~(df['distance_in_km'].isnull())
#     ]
#     duration_min = np.quantile(df['duration_in_min'], q=0.01)
#     duration_max = np.quantile(df['duration_in_min'], q=0.99)
#     distance_min = np.quantile(df['distance_in_km'], q=0.01)
#     distance_max = np.quantile(df['distance_in_km'], q=0.99)
#     df = df[(df['duration_in_min'] > duration_min) & (df['duration_in_min'] < duration_max)]
#     df = df[(df['distance_in_km'] > distance_min) & (df['distance_in_km'] < distance_max)]
    df['utc_dt'] = df['utc_order_dttm'].dt.date
    df['utc_hour'] = df['utc_order_dttm'].dt.hour
    df['utc_weekday'] = df['utc_order_dttm'].dt.weekday
    df['local_dt'] = df['local_order_dttm'].dt.date
    df['local_hour'] = df['local_order_dttm'].dt.hour
    df['local_weekday'] = df['local_order_dttm'].dt.weekday
    df['is_order_good'] = df['price_start_usd'] >= df['price_highrate_usd']
    df['is_order_with_tender'] = df['is_order_with_tender'].fillna(False)
    df['is_order_start_price_bid'] = df['is_order_start_price_bid'].fillna(False)
    df['is_order_accepted_start_price_bid'] = df['is_order_accepted_start_price_bid'].fillna(False)
    df['is_order_done_start_price_bid'] = df['is_order_done_start_price_bid'].fillna(False)
    df['is_order_accepted'] = df['is_order_accepted'].fillna(False)
    df['is_order_done'] = df['is_order_done'].fillna(False)
    df['is_order_good'] = df['is_order_good'].fillna(False)
    df = get_ts(df, date_column_name='local_order_dttm', by_time_resolution='30min')
    df['time'] = df['ts'].dt.time 
    # df = get_hex(df, hex_size=7)
    df.reset_index(drop=True, inplace=True)
    return df


def prepare_tender_data(df):
    df['group_name'] = df['tender_group_name']
#     df = df[
#         ~(df['duration_in_min'].isnull()) &
#         ~(df['distance_in_km'].isnull())
#     ]
#     duration_min = np.quantile(df['duration_in_min'], q=0.01)
#     duration_max = np.quantile(df['duration_in_min'], q=0.99)
#     distance_min = np.quantile(df['distance_in_km'], q=0.01)
#     distance_max = np.quantile(df['distance_in_km'], q=0.99)
#     df = df[(df['duration_in_min'] > duration_min) & (df['duration_in_min'] < duration_max)]
#     df = df[(df['distance_in_km'] > distance_min) & (df['distance_in_km'] < distance_max)]
    df['utc_dt'] = df['utc_order_dttm'].dt.date
    df['utc_hour'] = df['utc_order_dttm'].dt.hour
    df['utc_weekday'] = df['utc_order_dttm'].dt.weekday
    df['local_dt'] = df['local_order_dttm'].dt.date
    df['local_hour'] = df['local_order_dttm'].dt.hour
    df['local_weekday'] = df['local_order_dttm'].dt.weekday
    df['is_order_good'] = df['price_start_usd'] >= df['price_highrate_usd']
    df['is_order_with_tender'] = df['is_order_with_tender'].fillna(False)
    df['is_order_start_price_bid'] = df['is_order_start_price_bid'].fillna(False)
    df['is_order_accepted_start_price_bid'] = df['is_order_accepted_start_price_bid'].fillna(False)
    df['is_order_done_start_price_bid'] = df['is_order_done_start_price_bid'].fillna(False)
    df['is_order_accepted'] = df['is_order_accepted'].fillna(False)
    df['is_order_done'] = df['is_order_done'].fillna(False)
    df['is_order_good'] = df['is_order_good'].fillna(False)
    df = get_ts(df, date_column_name='local_order_dttm', by_time_resolution='30min')
    df['time'] = df['ts'].dt.time 
    df = get_hex(df, hex_size=7)
    df.reset_index(drop=True, inplace=True)
    return df


def get_orders_with_recprice_df(df_left, df_right):
    group_cols = ['calcprice_uuid']
    right_columns = set(df_right.columns) - (set(df_left.columns) & set(df_right.columns) - set(group_cols))
    df_right = df_right[list(right_columns)]
    df_full = df_left.merge(df_right, on=group_cols, how='left')
    df_full = df_full[round(df_full.recprice_usd, 3) == round(df_full.price_highrate_usd, 3)]
    print(f'только уникальные ордера? – {df_full.shape[0] == df_full.order_uuid.nunique()}')
    print(f'доля оставшихся ордеров: {round(df_full.order_uuid.nunique() / df_left.order_uuid.nunique(), 4)}')
    return df_full
