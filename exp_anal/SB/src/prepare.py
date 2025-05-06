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
    df['log_duration_in_sec'] = df['log_duration_in_min'] * 60
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
    df['duration_sec'] = df['duration_in_min'] * 60
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


def prepare_bid_data(df, t_param):
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

    # Store original row count
    original_count = len(df)
    
    # Drop rows with missing values in required columns
    df = df.dropna(
        subset=['eta', 'duration_in_min', 'price_highrate_value', 'price_start_value', 
                'available_prices_currency', 'is_bid_accepted', 'price_start_value']
    )
    
    # Calculate number of dropped rows
    dropped_count = original_count - len(df)
    
    # Print statistics
    print(f"Total rows: {original_count}")
    print(f"Dropped rows: {dropped_count} ({dropped_count/original_count:.2%})")
    print(f"Remaining rows: {len(df)}")
    
    # Add new columns
    df['price_diff'] = df.apply(
        lambda row: (max(row['available_prices_currency']) - row['price_start_value']) / row['price_start_value']  
        if isinstance(row['available_prices_currency'], (list, np.ndarray)) and len(row['available_prices_currency']) > 0
        else 0, 
        axis=1
    )
    df['unique_available_prices'] = df.apply(
        lambda row: len(row['available_prices_currency'])
        if isinstance(row['available_prices_currency'], (list, np.ndarray)) and len(row['available_prices_currency']) > 0
        else 0, 
        axis=1
    )

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

    df['bid2rec'] = df['bid_price_currency'] / df['price_highrate_value']
    df['max_eta_t'] = df['eta'].clip(lower=t_param)
    df['bidMPH'] = df['bid_price_currency'] / (df['max_eta_t'] + df['etr'])
    df['max_rec_start_price'] = df[['price_start_value', 'price_highrate_value']].max(axis=1)
    df['recMPH'] = df['max_rec_start_price'] / (t_param + df['etr'])
    df.drop('max_rec_start_price', axis=1, inplace=True)
    df['bidMPH2recMPH'] = df['bidMPH'] / df['recMPH']

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


def determine_bid_algorithm(row, t: float, alpha: float, 
                          groups={"control":"Control", "treatment":"A"},
                          coefficients_to_restore: list = [0.1, 0.2, 0.3]) -> str:
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
    max_price = np.nanmax([row['price_highrate_value'], row['price_start_value']])
    try:
        max_bid = int((1 + alpha) * max_price * (duration_seconds + eta) / (duration_seconds + t))
    except:
        print(f"""
          maxBid compute error
          max_price: {max_price}
          duration_seconds: {duration_seconds}
          eta: {eta}
          t: {t}
          """)
    
    # Шаг 2 и 3: проверяем available_prices
    if row['group_name'] == groups['treatment']:
        available_prices = row['price_start_value'] * (1 + np.array(coefficients_to_restore))
    else:
        available_prices = row['available_prices_currency']

    try:
        if max(available_prices) <= max_bid:
            return 'algo_default', {'eta': eta, 'duration_seconds': duration_seconds, 'max_price': max_price, 'max_bid': max_bid, 't': t, 'alpha': alpha, 'available_prices': available_prices}
        else:
            return 'algo_bidmph', {'eta': eta, 'duration_seconds': duration_seconds, 'max_price': max_price, 'max_bid': max_bid, 't': t, 'alpha': alpha, 'available_prices': available_prices}
    except:
        return 'algo assignment error'


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

