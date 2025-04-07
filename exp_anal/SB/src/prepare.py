import warnings
warnings.filterwarnings("ignore")

import h3
import numpy as np
import pandas as pd


def get_hex(df, hex_size):
    df[f'hex_from_calc_{hex_size}'] = [h3.latlng_to_cell(x, y, hex_size) for x, y in zip(df['fromlatitude'], df['fromlongitude'])]
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
    df = get_hex(df, hex_size=7)
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
    df = get_hex(df, hex_size=7)
    df.reset_index(drop=True, inplace=True)
    return df


def prepare_bid_data(df):
    df['group_name'] = df['bid_group_name']
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

    # Группируем по order_uuid и находим min_utc_bid_dttm
    min_times = df.groupby('order_uuid', as_index=False)['utc_bid_dttm'].min()
    min_times.rename(columns={'utc_bid_dttm': 'min_utc_bid_dttm'}, inplace=True)
    # Объединяем min_utc_bid_dttm с основным df
    df = df.merge(min_times, on='order_uuid', how='left')
    # Вычисляем новые поля
    df['time_to_1st_bid_sec'] = (df['min_utc_bid_dttm'] - df['utc_order_dttm']).dt.total_seconds()
    df['time_1st_bid_to_accept_sec'] = (df['bid_accept_utc_timestamp'] - df['min_utc_bid_dttm']).dt.total_seconds()
    # Удаляем временные переменные
    del min_times

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


def determine_bid_algorithm(row, t: float, alpha: float) -> str:
    """
    Определяет алгоритм ставок для каждой строки данных.
    
    Parameters:
    -----------
    row : pandas.Series
        Строка датафрейма с необходимыми полями:
        - eta: время ожидания
        - duration_in_min: estimated time to ride (в минутах)
        - price_highrate_value: рекомендованная цена
        - price_start_value: стартовая цена
        - available_prices_currency: доступные цены для ставок
    t : float
        Минимальное значение для eta
    alpha : float
        Коэффициент для расчета максимальной ставки
    
    Returns:
    --------
    str
        '' если max(available_prices) <= max_bid, иначе 'bid_mph'
    """
    # Шаг 0: проверяем eta
    eta = max(row['eta'], t)
    
    # Конвертируем длительность из минут в секунды
    duration_seconds = row['duration_in_min'] * 60
    
    # Шаг 1: вычисляем max_bid
    # max_price = max(row['price_highrate_value'], row['price_start_value'])
    max_price = np.nanmax([row['price_highrate_value'], row['price_start_value']])
    try:
        max_bid = int((1 + alpha) * max_price * (duration_seconds + eta) / (duration_seconds + t))
    except:
        print(f"""
          max_price: {max_price}
          duration_seconds: {duration_seconds}
          eta: {eta}
          t: {t}
          """)
    
    # Шаг 2 и 3: проверяем available_prices
    try:
        available_prices = row['available_prices_currency']
        if max(available_prices) <= max_bid:
            return 'algo_default', {'eta': eta, 'duration_seconds': duration_seconds, 'max_price': max_price, 'max_bid': max_bid, 't': t, 'alpha': alpha, 'available_prices': available_prices}
        else:
            return 'algo_bid_mph', {'eta': eta, 'duration_seconds': duration_seconds, 'max_price': max_price, 'max_bid': max_bid, 't': t, 'alpha': alpha, 'available_prices': available_prices}
    except:
        return 'error'


def add_algo_name_new(df: pd.DataFrame, t: float, alpha: float) -> pd.DataFrame:
    """
    Добавляет колонку 'algo_name_new' в датафрейм на основе определения алгоритма ставок.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Датафрейм с необходимыми полями
    t : float
        Минимальное значение для eta
    alpha : float
        Коэффициент для расчета максимальной ставки
    
    Returns:
    --------
    pandas.DataFrame
        Датафрейм с добавленной колонкой 'algo_name_new'
    """
    df['algo_name_new'] = df.apply(lambda row: determine_bid_algorithm(row, t, alpha)[0], axis=1)
    df['tmp'] = df.apply(lambda row: determine_bid_algorithm(row, t, alpha)[1], axis=1)
    return df

