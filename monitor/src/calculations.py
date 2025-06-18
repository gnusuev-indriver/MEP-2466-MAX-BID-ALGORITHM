import pandas as pd

# 2. Функция для доли строк с нужным bidding_algorithm_name
def share_bidding_algorithm(df):
    shares = df['bidding_algorithm_name'].value_counts(normalize=True)
    return shares.to_dict()

# 3. Функция для доли совпадений bid_price_currency с 1/2/3 значением в available_prices_currency
# def share_bid_price_matches(df, algo_names=['bid_mph_no_exposure_recalculated'], len_max=3):
#     filtered = df[df['bidding_algorithm_name'].isin(algo_names)]

#     def match_nth(n):
#         return filtered.apply(
#             lambda row: len(row['available_prices_currency']) > n and row['bid_price_currency'] == row['available_prices_currency'][n], axis=1
#         ).mean()

#     def match_sp():
#         return filtered.apply(
#             lambda row: row['bid_price_currency'] == row['price_start_value'], axis=1
#         ).mean()

#     # 1. Доли для каждого промежутка между значениями
#     def match_between(len_max = len_max):
#         # Считаем количество промежутков (максимальная длина - 1)
#         max_len = filtered['available_prices_currency'].apply(len).max()
#         between_shares = {}
#         for i in range(min(max_len - 1, len_max)):
#             between_shares[f'between_{i}_{i+1}'] = filtered.apply(
#                 lambda row: (
#                     len(row['available_prices_currency']) > i+1 and
#                     row['available_prices_currency'][i] < row['bid_price_currency'] < row['available_prices_currency'][i+1]
#                 ), axis=1
#             ).mean()
#         return between_shares

#     # 2. Доля случаев, когда bid_price_currency больше последнего available_prices_currency
#     def match_above_last():
#         return filtered.apply(
#             lambda row: (
#                 len(row['available_prices_currency']) > 0 and
#                 row['bid_price_currency'] > row['available_prices_currency'][-1]
#             ), axis=1
#         ).mean()

#     result = {
#         'match_sp': match_sp(),
#         'match_first': match_nth(0),
#         'match_second': match_nth(1),
#         'match_third': match_nth(2),
#     }
#     result.update(match_between())
#     result['above_last'] = match_above_last()
#     return result


# 4. Функция для доли длин листа available_prices_currency 3, 2, 1
def share_available_prices_length(df, algo_names=['bid_mph_no_exposure_recalculated']):
    filtered = df[df['bidding_algorithm_name'].isin(algo_names)]
    lengths = filtered['available_prices_currency'].apply(len)
    total = len(lengths)
    max_len = lengths.max()
    
    result = {}
    for i in range(max_len + 1):
        result[f'len_{i}'] = (lengths == i).mean()
    
    result['total'] = total
    return result

#5
def share_bid_price_matches_by_len(df, algo_names=['bid_mph_no_exposure_recalculated'], len_max=3):
    res = {}
    for i in range(min(df[df['bidding_algorithm_name'].isin(algo_names)]['available_prices_currency'].apply(len).max(), len_max)):
        filtered = df[(df['available_prices_currency'].apply(len) == i+1) & 
                      (df['bidding_algorithm_name'].isin(algo_names))]
        if len(filtered) > 0:
            res['len_'+str(i+1)] = share_bid_price_matches(filtered, algo_names=algo_names)
    return res








def share_bid_price_matches(df, algo_names=['bid_mph_no_exposure_recalculated'], len_max=3):
    filtered = df[df['bidding_algorithm_name'].isin(algo_names)]

    def match_nth(n):
        matches = filtered.apply(
            lambda row: len(row['available_prices_currency']) > n and row['bid_price_currency'] == row['available_prices_currency'][n], axis=1
        )
        return {
            'share': matches.mean(),
            'done_share': filtered[matches]['is_bid_done'].mean()
        }

    def match_sp():
        matches = filtered.apply(
            lambda row: row['bid_price_currency'] == row['price_start_value'], axis=1
        )
        return {
            'share': matches.mean(),
            'done_share': filtered[matches]['is_bid_done'].mean()
        }

    def match_between(len_max = len_max):
        max_len = filtered['available_prices_currency'].apply(len).max()
        between_shares = {}
        for i in range(min(max_len - 1, len_max)):
            matches = filtered.apply(
                lambda row: (
                    len(row['available_prices_currency']) > i+1 and
                    row['available_prices_currency'][i] < row['bid_price_currency'] < row['available_prices_currency'][i+1]
                ), axis=1
            )
            between_shares[f'between_{i}_{i+1}'] = {
                'share': matches.mean(),
                'done_share': filtered[matches]['is_bid_done'].mean()
            }
        return between_shares

    def match_above_last():
        matches = filtered.apply(
            lambda row: (
                len(row['available_prices_currency']) > 0 and
                row['bid_price_currency'] > row['available_prices_currency'][-1]
            ), axis=1
        )
        return {
            'share': matches.mean(),
            'done_share': filtered[matches]['is_bid_done'].mean()
        }

    result = {
        'match_sp': match_sp(),
        'match_first': match_nth(0),
        'match_second': match_nth(1),
        'match_third': match_nth(2),
    }
    result.update(match_between())
    result['above_last'] = match_above_last()
    return result