import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd

from .pipeline import RatioMetricHypothesisTestingPipeline

METRIC_LIST = [
    # market
    ["cp2order", "orders_count", "calcprices_count"],
    ["cp2bid", "start_price_bid_orders_count", "calcprices_count"],
    ["cp2start_price_bid", "start_price_bid_orders_count", "calcprices_count"],
    ["cp2accept", "accepted_orders_count", "calcprices_count"],
    ["cp2done", "rides_count", "calcprices_count"],
    ['drivers_per_order', 'drivers_count', 'orders_count'],
    ['bids_per_order', 'bids_count', 'orders_count'],
    ['bids_per_driver', 'bids_count', 'drivers_count'],
    ['bids_per_order_with_bid', 'bids_count', 'orders_with_bids_count'],
    ['bids_per_done_order', 'bids_for_done_orders_count', 'rides_count'],
    ['drivers_per_orders_with_bid', 'drivers_count', 'orders_with_bids_count'],
    ['bids_accepted_per_driver', 'accepted_bids_count', 'drivers_count'],

    # prices
    ["recprice_usd", "recprice_usd_sum", "calcprices_count"],
    ["price_highrate_usd", "price_highrate_usd_sum", "orders_count"],
    ["price_highrate_usd_rides", "rides_price_highrate_usd_sum", "rides_count"],
    ['price_highrate_usd_orders_without_bids', 'orders_without_bids_price_highrate_usd_sum', 'orders_without_bids_count'],
    ["price_start_usd", "price_start_usd_sum", "orders_count"],
    ["price_start_usd_rides", "rides_price_start_usd_sum", "rides_count"],
    ['price_start_usd_orders_without_bids', 'orders_without_bids_price_start_usd_sum', 'orders_without_bids_count'],
    ['price_bid_currency', 'bids_bid_price_currency_sum', 'bids_count'],
    ['price_bid_option_startprice', 'bids_option_startprice_bid_price_currency_sum', 'bids_option_startprice_count'],
    ['price_bid_option_option1', 'bids_option_option1_bid_price_currency_sum', 'bids_option_option1_count'],
    ['price_bid_option_option2', 'bids_option_option2_bid_price_currency_sum', 'bids_option_option2_count'],
    ['price_bid_option_option3', 'bids_option_option3_bid_price_currency_sum', 'bids_option_option3_count'],
    ['price_bid_option_other1', 'bids_option_other1_bid_price_currency_sum', 'bids_option_other1_count'],
    ['price_bid_option_other2', 'bids_option_other2_bid_price_currency_sum', 'bids_option_other2_count'],
    ['price_bid_option_other3', 'bids_option_other3_bid_price_currency_sum', 'bids_option_other3_count'],
    ["price_tender_usd", "price_tender_usd_sum", "tenders_count"],
    ["price_done_usd", "price_done_usd_sum", "rides_count"],
    
    # orders
    ## order2bid
    ["order2bid", "orders_with_bids_count", "orders_count"], #MR
    ['order2bid_option_startprice', 'orders_with_bid_option_startprice_count', 'orders_count'],
    ['order2bid_option_option1', 'orders_with_bid_option_option1_count', 'orders_count'],
    ['order2bid_option_option2', 'orders_with_bid_option_option2_count', 'orders_count'],
    ['order2bid_option_option3', 'orders_with_bid_option_option3_count', 'orders_count'],
    ['order2bid_option_other1', 'orders_with_bid_option_other1_count', 'orders_count'],
    ['order2bid_option_other2', 'orders_with_bid_option_other2_count', 'orders_count'],
    ['order2bid_option_other3', 'orders_with_bid_option_other3_count', 'orders_count'],
    ## order2accept
    ["order2accept", "accepted_orders_count", "orders_count"],
    ['order2accept_option_startprice', 'accepted_bids_option_startprice_count', 'orders_with_bid_option_startprice_count'],
    ['order2accept_option_option1', 'accepted_bids_option_option1_count', 'orders_with_bid_option_option1_count'],
    ['order2accept_option_option2', 'accepted_bids_option_option2_count', 'orders_with_bid_option_option2_count'],
    ['order2accept_option_option3', 'accepted_bids_option_option3_count', 'orders_with_bid_option_option3_count'],
    ['order2accept_option_other1', 'accepted_bids_option_other1_count', 'orders_with_bid_option_other1_count'],
    ['order2accept_option_other2', 'accepted_bids_option_other2_count', 'orders_with_bid_option_other2_count'],
    ['order2accept_option_other3', 'accepted_bids_option_other3_count', 'orders_with_bid_option_other3_count'],
    ## order2done
    ["order2done", "rides_count", "orders_count"], #DR
    ['order2done_bid_option_startprice', 'rides_by_bid_option_startprice_count', 'orders_count'],
    ['order2done_bid_option_option1', 'rides_by_bid_option_option1_count', 'orders_count'],
    ['order2done_bid_option_option2', 'rides_by_bid_option_option2_count', 'orders_count'],
    ['order2done_bid_option_option3', 'rides_by_bid_option_option3_count', 'orders_count'],
    ['order2done_bid_option_other1', 'rides_by_bid_option_other1_count', 'orders_count'],
    ['order2done_bid_option_other2', 'rides_by_bid_option_other2_count', 'orders_count'],
    ['order2done_bid_option_other3', 'rides_by_bid_option_other3_count', 'orders_count'],
    ## bid2accept
    ["bid2accept", "accepted_orders_count", "orders_with_bids_count"],
    ['bid_option_startprice2accept', 'accepted_bids_option_startprice_count', 'orders_with_bid_option_startprice_count'],
    ['bid_option_option12accept', 'accepted_bids_option_option1_count', 'orders_with_bid_option_option1_count'],
    ['bid_option_option22accept', 'accepted_bids_option_option2_count', 'orders_with_bid_option_option2_count'],
    ['bid_option_option32accept', 'accepted_bids_option_option3_count', 'orders_with_bid_option_option3_count'],
    ['bid_option_other12accept', 'accepted_bids_option_other1_count', 'orders_with_bid_option_other1_count'],
    ['bid_option_other22accept', 'accepted_bids_option_other2_count', 'orders_with_bid_option_other2_count'],
    ['bid_option_other32accept', 'accepted_bids_option_other3_count', 'orders_with_bid_option_other3_count'],
    ## accept2done
    ["accept2done", "rides_count", "accepted_orders_count"],
    ['accepted_bids_option_startprice2done', 'rides_by_bid_option_startprice_count', 'accepted_bids_option_startprice_count'],
    ['accepted_bids_option_option12done', 'rides_by_bid_option_option1_count', 'accepted_bids_option_option1_count'],
    ['accepted_bids_option_option22done', 'rides_by_bid_option_option2_count', 'accepted_bids_option_option2_count'],
    ['accepted_bids_option_option32done', 'rides_by_bid_option_option3_count', 'accepted_bids_option_option3_count'],
    ['accepted_bids_option_other12done', 'rides_by_bid_option_other1_count', 'accepted_bids_option_other1_count'],
    ['accepted_bids_option_other22done', 'rides_by_bid_option_other2_count', 'accepted_bids_option_other2_count'],
    ['accepted_bids_option_other32done', 'rides_by_bid_option_other3_count', 'accepted_bids_option_other3_count'],
    ## bid2done
    ["bid2done", "rides_count", "orders_with_bids_count"],
    ['bid_option_startprice2done', 'rides_by_bid_option_startprice_count', 'orders_with_bid_option_startprice_count'],
    ['bid_option_option12done', 'rides_by_bid_option_option1_count', 'orders_with_bid_option_option1_count'],
    ['bid_option_option22done', 'rides_by_bid_option_option2_count', 'orders_with_bid_option_option2_count'],
    ['bid_option_option32done', 'rides_by_bid_option_option3_count', 'orders_with_bid_option_option3_count'],
    ['bid_option_other12done', 'rides_by_bid_option_other1_count', 'orders_with_bid_option_other1_count'],
    ['bid_option_other22done', 'rides_by_bid_option_other2_count', 'orders_with_bid_option_other2_count'],
    ['bid_option_other32done', 'rides_by_bid_option_other3_count', 'orders_with_bid_option_other3_count'],

    # bids
    ## options shares
    ['bids_option_startprice_share', 'bids_option_startprice_count', 'bids_count'],
    ['bids_option_option1_share', 'bids_option_option1_count', 'bids_count'],
    ['bids_option_option2_share', 'bids_option_option2_count', 'bids_count'],
    ['bids_option_option3_share', 'bids_option_option3_count', 'bids_count'],
    ['bids_option_other1_share', 'bids_option_other1_count', 'bids_count'],
    ['bids_option_other2_share', 'bids_option_other2_count', 'bids_count'],
    ['bids_option_other3_share', 'bids_option_other3_count', 'bids_count'],
    ## share of accepted 
    ['accepted_bids_share', 'accepted_bids_count', 'bids_count'],
    ['accepted_share_bid_option_startprice', 'accepted_bids_option_startprice_count', 'bids_option_startprice_count'],
    ['accepted_share_bid_option_option1', 'accepted_bids_option_option1_count', 'bids_option_option1_count'],
    ['accepted_share_bid_option_option2', 'accepted_bids_option_option2_count', 'bids_option_option2_count'],
    ['accepted_share_bid_option_option3', 'accepted_bids_option_option3_count', 'bids_option_option3_count'],
    ['accepted_share_bid_option_other1', 'accepted_bids_option_other1_count', 'bids_option_other1_count'],
    ['accepted_share_bid_option_other2', 'accepted_bids_option_other2_count', 'bids_option_other2_count'],
    ['accepted_share_bid_option_other3', 'accepted_bids_option_other3_count', 'bids_option_other3_count'],
    ## share of done 
    ['done_share_bid_option_startprice', 'rides_by_bid_option_startprice_count', 'accepted_bids_option_startprice_count'],
    ['done_share_bid_option_option1', 'rides_by_bid_option_option1_count', 'accepted_bids_option_option1_count'],
    ['done_share_bid_option_option2', 'rides_by_bid_option_option2_count', 'accepted_bids_option_option2_count'],
    ['done_share_bid_option_option3', 'rides_by_bid_option_option3_count', 'accepted_bids_option_option3_count'],
    ['done_share_bid_option_other1', 'rides_by_bid_option_other1_count', 'accepted_bids_option_other1_count'],
    ['done_share_bid_option_other2', 'rides_by_bid_option_other2_count', 'accepted_bids_option_other2_count'],
    ['done_share_bid_option_other3', 'rides_by_bid_option_other3_count', 'accepted_bids_option_other3_count'],

    # times
    ['eta', 'eta_sum', 'bids_count'],
    ['rta', 'rta_sum', 'rides_count'],
    ['etr', 'etr_sum', 'rides_count'],
    ['rtr', 'rtr_sum', 'rides_count'],
    ['eta_done_orders', 'eta_done_orders_sum', 'bids_count'],
    ['eta_accepted_bids', 'eta_accepted_bids_sum', 'bids_count'],
    ['etr_orders_with_bids', 'etr_orders_with_bids_sum', 'rides_count'],
    ['etr_orders_with_accepted_bids', 'etr_orders_with_accepted_bids_sum', 'rides_count'],
    ['etr_done_orders', 'etr_done_orders_sum', 'rides_count'],
    ['etr_orders_without_bids', 'etr_orders_without_bids_sum', 'rides_count'],
    ['time_to_1st_bid', 'time_to_1st_bid_sec', 'orders_with_bids_count'],
    ['time_1st_bid_to_accept', 'time_1st_bid_to_accept_sec', 'accepted_bids_count'],

    

    # # conversions
    
    # ### order --> ...
    # ["order2start_price_bid", "start_price_bid_orders_count", "orders_count"],
    # ### bid --> ...
    # ["start_price_bid2accept", "start_price_bid_accepted_orders_count", "start_price_bid_orders_count"],
    # ["start_price_bid2done", "start_price_bid_rides_count", "start_price_bid_orders_count"],
    # ### kinda duplicates
    # ['bids_accepted_per_order', 'accepted_bids_count', 'orders_count'],
    # ['bids_accepted_per_order_with_bid', 'accepted_bids_count', 'orders_with_bids_count'],
    
    # # prices
    # ["minprice_usd", "minprice_usd_sum", "calcprices_count"],
    # ["price_base_usd", "price_base_usd_sum", "calcprices_count"],
    # # surge
    # ["surge", "surge_sum", "calcprices_count"],
    # ["surge_gr_1", "surge_gr_1_sum", "surge_gr_1_calcprices_count"],
    # ["surge_le_1", "surge_le_1_sum", "surge_le_1_calcprices_count"],
    # ["surge_gr_1_coverage", "surge_gr_1_calcprices_count", "calcprices_count"],
    # ["surge_le_1_coverage", "surge_le_1_calcprices_count", "calcprices_count"],
    # # balance
    # ["good_rate", "good_orders_count", "orders_count"],
    # ["balance", "good_orders_count", "rides_count"],
    # # by segment
    # ### segment: minprice
    # ["orders_by_minprice_share", "orders_by_minprice_count", "orders_count"],
    # ["cp2order_by_orders_by_minprice", "orders_by_minprice_count", "calcprices_count"],
    # ["order2done_by_orders_by_minprice", "rides_by_minprice_count", "orders_by_minprice_count"],
    # ["cp2done_by_orders_by_minprice", "rides_by_minprice_count", "calcprices_count"],
    # ### segment: surge
    # ["surge_gr_1_orders_share", "surge_gr_1_orders_count", "orders_count"],
    # ["surge_le_1_orders_share", "surge_le_1_orders_count", "orders_count"],
    # ["surge_gr_1_rides_share", "surge_gr_1_rides_count", "rides_count"],
    # ["surge_le_1_rides_share", "surge_le_1_rides_count", "rides_count"],
    # ["surge_gr_1_cp2order", "surge_gr_1_orders_count", "surge_gr_1_calcprices_count"],
    # ["surge_le_1_cp2order", "surge_le_1_orders_count", "surge_le_1_calcprices_count"],
    # ["surge_gr_1_order2done", "surge_gr_1_rides_count", "surge_gr_1_orders_count"],
    # ["surge_le_1_order2done", "surge_le_1_rides_count", "surge_le_1_orders_count"],
    # ["surge_gr_1_cp2done", "surge_gr_1_rides_count", "surge_gr_1_calcprices_count"],
    # ["surge_le_1_cp2done", "surge_le_1_rides_count", "surge_le_1_calcprices_count"]   
]


def get_switchback_results(df, alpha, metric_list=METRIC_LIST, groups={"control":"Control", "treatment":"A"}):
    res_list = []
    for i in metric_list:
        pipeline = RatioMetricHypothesisTestingPipeline(df, i[0], i[1], i[2], groups)
        pipeline.run()
        res_list.append(pipeline.result)
    df_res = pd.DataFrame(res_list)
    df_res[f'is_significant'] = df_res['pvalue'] < alpha
    return df_res


# from `indriver-e6e40.emart.incity_detail`
def metric_orders_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(orders_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_tenders_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(tenders_count=('tenders_count', 'sum'))
            .reset_index())

def metric_orders_with_bids_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(orders_with_bids_count=('is_order_with_tender', 'sum'))
            .reset_index())

def metric_orders_without_bids_count(df, group_cols):
    return (df[~df.is_order_with_tender]
            .groupby(group_cols)
            .size()
            .reset_index(name='orders_without_bids_count'))

def metric_start_price_bid_orders_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(start_price_bid_orders_count=('is_order_start_price_bid', 'sum'))
            .reset_index())

def metric_start_price_bid_accepted_orders_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(start_price_bid_accepted_orders_count=('is_order_accepted_start_price_bid', 'sum'))
            .reset_index())

def metric_start_price_bid_rides_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(start_price_bid_rides_count=('is_order_done_start_price_bid', 'sum'))
            .reset_index())

def metric_accepted_orders_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(accepted_orders_count=('is_order_accepted', 'sum'))
            .reset_index())

def metric_rides_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(rides_count=('is_order_done', 'sum'))
            .reset_index())

def metric_price_start_usd_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(price_start_usd_sum=('price_start_usd', 'sum'))
            .reset_index())

def metric_orders_without_bids_price_start_usd_sum(df, group_cols):
    return (df[~df.is_order_with_tender]
            .groupby(group_cols)
            .agg(orders_without_bids_price_start_usd_sum=('price_start_usd', 'sum'))
            .reset_index())

def metric_rides_price_start_usd_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(rides_price_start_usd_sum=('rides_price_start_usd', 'sum'))
            .reset_index())

def metric_price_highrate_usd_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(price_highrate_usd_sum=('price_highrate_usd', 'sum'))
            .reset_index())

def metric_orders_without_bids_price_highrate_usd_sum(df, group_cols):
    return (df[~df.is_order_with_tender]
            .groupby(group_cols)
            .agg(orders_without_bids_price_highrate_usd_sum=('price_highrate_usd', 'sum'))
            .reset_index())

def metric_rides_price_highrate_usd_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(rides_price_highrate_usd_sum=('rides_price_highrate_usd', 'sum'))
            .reset_index())

def metric_price_tender_usd_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(price_tender_usd_sum=('price_tender_usd', 'sum'))
            .reset_index())

def metric_price_done_usd_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(price_done_usd_sum=('price_done_usd', 'sum'))
            .reset_index())

def metric_good_orders_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(good_orders_count=('is_order_good', 'sum'))
            .reset_index())


# from `indriver-e6e40.ods_recprice_cdc.pricing_logs`
def metric_calcprices_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(calcprices_count=('calcprice_uuid', 'nunique'))
            .reset_index())

def metric_price_base_usd_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(price_base_usd_sum=('price_base_usd', 'sum'))
            .reset_index())

def metric_recprice_usd_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(recprice_usd_sum=('recprice_usd', 'sum'))
            .reset_index())

def metric_minprice_usd_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(minprice_usd_sum=('minprice_usd', 'sum'))
            .reset_index())

def metric_surge_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(surge_sum=('surge', 'sum'))
            .reset_index())

def metric_surge_gr_1_sum(df, group_cols):
    return (df[df.surge > 1]
            .groupby(group_cols)
            .agg(surge_gr_1_sum=('surge', 'sum'))
            .reset_index())

def metric_surge_gr_1_calcprices_count(df, group_cols):
    return (df[df.surge > 1]
            .groupby(group_cols)
            .agg(surge_gr_1_calcprices_count=('calcprice_uuid', 'nunique'))
            .reset_index())

def metric_surge_le_1_sum(df, group_cols):
    return (df[df.surge <= 1]
            .groupby(group_cols)
            .agg(surge_le_1_sum=('surge', 'sum'))
            .reset_index())

def metric_surge_le_1_calcprices_count(df, group_cols):
    return (df[df.surge <= 1]
            .groupby(group_cols)
            .agg(surge_le_1_calcprices_count=('calcprice_uuid', 'nunique'))
            .reset_index())


# from incity_and_pricing_tbl
def metric_orders_by_minprice_count(df, group_cols):
    return (df[(df.price_start_usd >= df.minprice_usd*0.99) & (df.price_start_usd <= df.minprice_usd*1.01)]
            .groupby(group_cols)
            .agg(orders_by_minprice_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_orders_by_minprice_with_bids_count(df, group_cols):
    return (df[(df.price_start_usd >= df.minprice_usd*0.99) & (df.price_start_usd <= df.minprice_usd*1.01) & (df.is_order_with_tender)]
            .groupby(group_cols)
            .agg(orders_by_minprice_with_bids_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_accepted_orders_by_minprice_count(df, group_cols):
    return (df[(df.price_start_usd >= df.minprice_usd*0.99) & (df.price_start_usd <= df.minprice_usd*1.01) & (df.is_order_accepted)]
            .groupby(group_cols)
            .agg(accepted_orders_by_minprice_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_rides_by_minprice_count(df, group_cols):
    return (df[(df.price_start_usd >= df.minprice_usd*0.99) & (df.price_start_usd <= df.minprice_usd*1.01) & (df.is_order_done)]
            .groupby(group_cols)
            .agg(rides_by_minprice_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_gr_1_orders_count(df, group_cols):
    return (df[df.surge > 1]
            .groupby(group_cols)
            .agg(surge_gr_1_orders_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_le_1_orders_count(df, group_cols):
    return (df[df.surge <= 1]
            .groupby(group_cols)
            .agg(surge_le_1_orders_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_gr_1_orders_with_bids_count(df, group_cols):
    return (df[(df.surge > 1) & (df.is_order_with_tender)]
            .groupby(group_cols)
            .agg(surge_gr_1_orders_with_bids_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_le_1_orders_with_bids_count(df, group_cols):
    return (df[(df.surge <= 1) & (df.is_order_with_tender)]
            .groupby(group_cols)
            .agg(surge_le_1_orders_with_bids_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_gr_1_start_price_bid_orders_count(df, group_cols):
    return (df[(df.surge > 1) & (df.is_order_start_price_bid)]
            .groupby(group_cols)
            .agg(surge_gr_1_start_price_bid_orders_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_le_1_start_price_bid_orders_count(df, group_cols):
    return (df[(df.surge <= 1) & (df.is_order_start_price_bid)]
            .groupby(group_cols)
            .agg(surge_le_1_start_price_bid_orders_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_gr_1_accepted_orders_count(df, group_cols):
    return (df[(df.surge > 1) & (df.is_order_accepted)]
            .groupby(group_cols)
            .agg(surge_gr_1_accepted_orders_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_le_1_accepted_orders_count(df, group_cols):
    return (df[(df.surge <= 1) & (df.is_order_accepted)]
            .groupby(group_cols)
            .agg(surge_le_1_accepted_orders_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_gr_1_rides_count(df, group_cols):
    return (df[(df.surge > 1) & (df.is_order_done)]
            .groupby(group_cols)
            .agg(surge_gr_1_rides_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_le_1_rides_count(df, group_cols):
    return (df[(df.surge <= 1) & (df.is_order_done)]
            .groupby(group_cols)
            .agg(surge_le_1_rides_count=('order_uuid', 'nunique'))
            .reset_index())


# from df_bids
def metric_drivers_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(drivers_count=('driver_uuid', 'nunique'))
            .reset_index())

def metric_bids_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(bids_count=('bid_uuid', 'nunique'))
            .reset_index())

def metric_bids_for_done_orders_count(df, group_cols):
    return (df[df.is_order_done]
            .groupby(group_cols)
            .agg(bids_for_done_orders_count=('bid_uuid', 'nunique'))
            .reset_index())

def metric_bids_bid_price_currency_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(bids_bid_price_currency_sum=('bid_price_currency', 'sum'))
            .reset_index())

def metric_accepted_bids_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(accepted_bids_count=('is_bid_accepted', 'sum'))
            .reset_index())

def metric_accepted_bids_bid_price_currency_sum(df, group_cols):
    return (df[df.is_bid_accepted]
            .groupby(group_cols)
            .agg(accepted_bids_bid_price_currency_sum=('bid_price_currency', 'sum'))
            .reset_index())

def metric_orders_with_bid_option_startprice_count(df, group_cols):
    return (df[(df.option_number == 'startprice')]
            .groupby(group_cols)
            .agg(orders_with_bid_option_startprice_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_rides_by_bid_option_startprice_count(df, group_cols):
    return (df[(df.option_number == 'startprice') & (df.is_bid_accepted) & (df.is_order_done)]
            .groupby(group_cols)
            .agg(rides_by_bid_option_startprice_count=('order_uuid', 'nunique'))
            .reset_index())

def TEST_rides_count_by_bids(df, group_cols):
    return (df[df.is_order_done]
            .groupby(group_cols)
            .agg(TEST_rides_count_by_bids=('order_uuid', 'nunique'))
            .reset_index())

def metric_bids_option_startprice_count(df, group_cols):
    return (df[df.option_number == 'startprice']
            .groupby(group_cols)
            .agg(bids_option_startprice_count=('bid_uuid', 'nunique'))
            .reset_index())

def metric_bids_option_startprice_bid_price_currency_sum(df, group_cols):
    return (df[df.option_number == 'startprice']
            .groupby(group_cols)
            .agg(bids_option_startprice_bid_price_currency_sum=('bid_price_currency', 'sum'))
            .reset_index())

def metric_accepted_bids_option_startprice_count(df, group_cols):
    return (df[(df.option_number == 'startprice') & (df.is_bid_accepted)]
            .groupby(group_cols)
            .agg(accepted_bids_option_startprice_count=('bid_uuid', 'nunique'))
            .reset_index())

def metric_bids_option_startprice_accepted_bid_price_currency_sum(df, group_cols):
    return (df[(df.option_number == 'startprice') & (df.is_bid_accepted)]
            .groupby(group_cols)
            .agg(accepted_bids_option_startprice_bid_price_currency_sum=('bid_price_currency', 'sum'))
            .reset_index())

def metric_orders_with_bid_option_option1_count(df, group_cols):
    return (df[(df.option_number == 'option 1')]
            .groupby(group_cols)
            .agg(orders_with_bid_option_option1_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_rides_by_bid_option_option1_count(df, group_cols):
    return (df[(df.option_number == 'option 1') & (df.is_bid_accepted) & (df.is_order_done)]
            .groupby(group_cols)
            .agg(rides_by_bid_option_option1_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_bids_option_option1_count(df, group_cols):
    return (df[df.option_number == 'option 1']
            .groupby(group_cols)
            .agg(bids_option_option1_count=('bid_uuid', 'nunique'))
            .reset_index())

def metric_bids_option_option1_bid_price_currency_sum(df, group_cols):
    return (df[df.option_number == 'option 1']
            .groupby(group_cols)
            .agg(bids_option_option1_bid_price_currency_sum=('bid_price_currency', 'sum'))
            .reset_index())

def metric_accepted_bids_option_option1_count(df, group_cols):
    return (df[(df.option_number == 'option 1') & (df.is_bid_accepted)]
            .groupby(group_cols)
            .agg(accepted_bids_option_option1_count=('bid_uuid', 'nunique'))
            .reset_index())

def metric_accepted_bids_option_option1_bid_price_currency_sum(df, group_cols):
    return (df[(df.option_number == 'option 1') & (df.is_bid_accepted)]
            .groupby(group_cols)
            .agg(accepted_bids_option_option1_bid_price_currency_sum=('bid_price_currency', 'sum'))
            .reset_index())

def metric_orders_with_bid_option_option2_count(df, group_cols):
    return (df[(df.option_number == 'option 2')]
            .groupby(group_cols)
            .agg(orders_with_bid_option_option2_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_rides_by_bid_option_option2_count(df, group_cols):
    return (df[(df.option_number == 'option 2') & (df.is_bid_accepted) & (df.is_order_done)]
            .groupby(group_cols)
            .agg(rides_by_bid_option_option2_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_bids_option_option2_count(df, group_cols):
    return (df[df.option_number == 'option 2']
            .groupby(group_cols)
            .agg(bids_option_option2_count=('bid_uuid', 'nunique'))
            .reset_index())

def metric_bids_option_option2_bid_price_currency_sum(df, group_cols):
    return (df[df.option_number == 'option 2']
            .groupby(group_cols)
            .agg(bids_option_option2_bid_price_currency_sum=('bid_price_currency', 'sum'))
            .reset_index())

def metric_accepted_bids_option_option2_count(df, group_cols):
    return (df[(df.option_number == 'option 2') & (df.is_bid_accepted)]
            .groupby(group_cols)
            .agg(accepted_bids_option_option2_count=('bid_uuid', 'nunique'))
            .reset_index())

def metric_accepted_bids_option_option2_bid_price_currency_sum(df, group_cols):
    return (df[(df.option_number == 'option 2') & (df.is_bid_accepted)]
            .groupby(group_cols)
            .agg(accepted_bids_option_option2_bid_price_currency_sum=('bid_price_currency', 'sum'))
            .reset_index())

def metric_orders_with_bid_option_option3_count(df, group_cols):
    return (df[(df.option_number == 'option 3')]
            .groupby(group_cols)
            .agg(orders_with_bid_option_option3_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_rides_by_bid_option_option3_count(df, group_cols):
    return (df[(df.option_number == 'option 3') & (df.is_bid_accepted) & (df.is_order_done)]
            .groupby(group_cols)
            .agg(rides_by_bid_option_option3_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_bids_option_option3_count(df, group_cols):
    return (df[df.option_number == 'option 3']
            .groupby(group_cols)
            .agg(bids_option_option3_count=('bid_uuid', 'nunique'))
            .reset_index())

def metric_bids_option_option3_bid_price_currency_sum(df, group_cols):
    return (df[df.option_number == 'option 3']
            .groupby(group_cols)
            .agg(bids_option_option3_bid_price_currency_sum=('bid_price_currency', 'sum'))
            .reset_index())

def metric_accepted_bids_option_option3_count(df, group_cols):
    return (df[(df.option_number == 'option 3') & (df.is_bid_accepted)]
            .groupby(group_cols)
            .agg(accepted_bids_option_option3_count=('bid_uuid', 'nunique'))
            .reset_index())

def metric_accepted_bids_option_option3_bid_price_currency_sum(df, group_cols):
    return (df[(df.option_number == 'option 3') & (df.is_bid_accepted)]
            .groupby(group_cols)
            .agg(accepted_bids_option_option3_bid_price_currency_sum=('bid_price_currency', 'sum'))
            .reset_index())

def metric_orders_with_bid_option_other1_count(df, group_cols):
    return (df[(df.option_number == 'option 1+')]
            .groupby(group_cols)
            .agg(orders_with_bid_option_other1_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_rides_by_bid_option_other1_count(df, group_cols):
    return (df[(df.option_number == 'option 1+') & (df.is_bid_accepted) & (df.is_order_done)]
            .groupby(group_cols)
            .agg(rides_by_bid_option_other1_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_bids_option_other1_count(df, group_cols):
    return (df[df.option_number == 'option 1+']
            .groupby(group_cols)
            .agg(bids_option_other1_count=('bid_uuid', 'nunique'))
            .reset_index())

def metric_bids_option_other1_bid_price_currency_sum(df, group_cols):
    return (df[df.option_number == 'option 1+']
            .groupby(group_cols)
            .agg(bids_option_other1_bid_price_currency_sum=('bid_price_currency', 'sum'))
            .reset_index())

def metric_accepted_bids_option_other1_count(df, group_cols):
    return (df[(df.option_number == 'option 1+') & (df.is_bid_accepted)]
            .groupby(group_cols)
            .agg(accepted_bids_option_other1_count=('bid_uuid', 'nunique'))
            .reset_index())

def metric_accepted_bids_option_other1_bid_price_currency_sum(df, group_cols):
    return (df[(df.option_number == 'option 1+') & (df.is_bid_accepted)]
            .groupby(group_cols)
            .agg(accepted_bids_option_other1_bid_price_currency_sum=('bid_price_currency', 'sum'))
            .reset_index())

def metric_orders_with_bid_option_other2_count(df, group_cols):
    return (df[(df.option_number == 'option 2+')]
            .groupby(group_cols)
            .agg(orders_with_bid_option_other2_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_rides_by_bid_option_other2_count(df, group_cols):
    return (df[(df.option_number == 'option 2+') & (df.is_bid_accepted) & (df.is_order_done)]
            .groupby(group_cols)
            .agg(rides_by_bid_option_other2_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_bids_option_other2_count(df, group_cols):
    return (df[df.option_number == 'option 2+']
            .groupby(group_cols)
            .agg(bids_option_other2_count=('bid_uuid', 'nunique'))
            .reset_index())

def metric_bids_option_other2_bid_price_currency_sum(df, group_cols):
    return (df[df.option_number == 'option 2+']
            .groupby(group_cols)
            .agg(bids_option_other2_bid_price_currency_sum=('bid_price_currency', 'sum'))
            .reset_index())

def metric_accepted_bids_option_other2_count(df, group_cols):
    return (df[(df.option_number == 'option 2+') & (df.is_bid_accepted)]
            .groupby(group_cols)
            .agg(accepted_bids_option_other2_count=('bid_uuid', 'nunique'))
            .reset_index())

def metric_accepted_bids_option_other2_bid_price_currency_sum(df, group_cols):
    return (df[(df.option_number == 'option 2+') & (df.is_bid_accepted)]
            .groupby(group_cols)
            .agg(accepted_bids_option_other2_bid_price_currency_sum=('bid_price_currency', 'sum'))
            .reset_index())

def metric_orders_with_bid_option_other3_count(df, group_cols):
    return (df[(df.option_number == 'option 3+')]
            .groupby(group_cols)
            .agg(orders_with_bid_option_other3_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_rides_by_bid_option_other3_count(df, group_cols):
    return (df[(df.option_number == 'option 3+') & (df.is_bid_accepted) & (df.is_order_done)]
            .groupby(group_cols)
            .agg(rides_by_bid_option_other3_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_bids_option_other3_count(df, group_cols):
    return (df[df.option_number == 'option 3+']
            .groupby(group_cols)
            .agg(bids_option_other3_count=('bid_uuid', 'nunique'))
            .reset_index())

def metric_bids_option_other3_bid_price_currency_sum(df, group_cols):
    return (df[df.option_number == 'option 3+']
            .groupby(group_cols)
            .agg(bids_option_other3_bid_price_currency_sum=('bid_price_currency', 'sum'))
            .reset_index())

def metric_accepted_bids_option_other3_count(df, group_cols):
    return (df[(df.option_number == 'option 3+') & (df.is_bid_accepted)]
            .groupby(group_cols)
            .agg(accepted_bids_option_other3_count=('bid_uuid', 'nunique'))
            .reset_index())

def metric_accepted_bids_option_other3_bid_price_currency_sum(df, group_cols):
    return (df[(df.option_number == 'option 3+') & (df.is_bid_accepted)]
            .groupby(group_cols)
            .agg(accepted_bids_option_other3_bid_price_currency_sum=('bid_price_currency', 'sum'))
            .reset_index())


# time metrics
def metric_time_to_1st_bid_sec(df, group_cols):
    min_times = df.groupby('order_uuid').agg({
        'utc_bid_dttm': 'min', 
        'utc_order_dttm': 'min'
        })
    min_times['time_diff'] = (min_times['utc_bid_dttm'] - min_times['utc_order_dttm']).dt.total_seconds()
    merged_df = df.merge(min_times[['time_diff']], on='order_uuid', how='left')
    return (merged_df
            .groupby(group_cols)
            .agg(time_to_1st_bid_sec=('time_diff', 'sum'))
            .reset_index())

def metric_time_1st_bid_to_accept_sec(df, group_cols):
    min_bid_times = df.groupby('order_uuid').agg({
        'utc_bid_dttm': 'min'
        }).rename(columns={'utc_bid_dttm': 'min_utc_bid_dttm'})
    accepted_bid_times = df[df.is_bid_accepted].groupby('order_uuid').agg({
        'bid_accept_utc_timestamp': 'first'
        }).rename(columns={'bid_accept_utc_timestamp': 'accepted_bid_utc_dttm'})
    combined_times = min_bid_times.join(accepted_bid_times, how='inner')
    combined_times['time_diff'] = (combined_times['accepted_bid_utc_dttm'] - 
                                   combined_times['min_utc_bid_dttm']).dt.total_seconds()
    merged_df = df.merge(combined_times[['time_diff']], on='order_uuid', how='left')
    return (merged_df
            .groupby(group_cols)
            .agg(time_1st_bid_to_accept_sec=('time_diff', 'sum'))
            .reset_index())

def metric_rta_sum(df, group_cols):
    return (df[df.is_order_done]
            .groupby(group_cols)
            .agg(rta_sum=('rta', 'sum'))
            .reset_index())

def metric_rtr_sum(df, group_cols):
    return (df[df.is_order_done]
            .groupby(group_cols)
            .agg(rtr_sum=('rtr', 'sum'))
            .reset_index())

def metric_eta_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(eta_sum=('eta', 'sum'))
            .reset_index())

def metric_eta_accepted_bids_sum(df, group_cols):
    return (df[df.is_bid_accepted]
            .groupby(group_cols)
            .agg(eta_accepted_bids_sum=('eta', 'sum'))
            .reset_index())

def metric_eta_done_orders_sum(df, group_cols):
    return (df[df.is_bid_accepted & df.is_order_done]
            .groupby(group_cols)
            .agg(eta_done_orders_sum=('eta', 'sum'))
            .reset_index())

def metric_etr_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(etr_sum=('etr', 'sum'))
            .reset_index())

def metric_etr_orders_without_bids_sum(df, group_cols):
    return (df[~df.is_order_with_tender]
            .groupby(group_cols)
            .agg(etr_orders_without_bids_sum=('etr', 'sum'))
            .reset_index())

def metric_etr_orders_with_bids_sum(df, group_cols):
    return (df[df.is_order_with_tender]
            .groupby(group_cols)
            .agg(etr_orders_with_bids_sum=('etr', 'sum'))
            .reset_index())

def metric_etr_orders_with_accepted_bids_sum(df, group_cols):
    return (df[df.is_order_with_tender & df.is_order_accepted]
            .groupby(group_cols)
            .agg(etr_orders_with_accepted_bids_sum=('etr', 'sum'))
            .reset_index())

def metric_etr_done_orders_sum(df, group_cols):
    return (df[df.is_order_done]
            .groupby(group_cols)
            .agg(etr_done_orders_sum=('etr', 'sum'))
            .reset_index())




def calculate_absolute_metrics(df_recprice, df_order_with_recprice, df_bid, group_cols):
    dfm = (metric_calcprices_count(df_recprice, group_cols)
           .merge(metric_orders_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_orders_without_bids_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_drivers_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_bids_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_bids_for_done_orders_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_bids_bid_price_currency_sum(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_accepted_bids_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_accepted_bids_bid_price_currency_sum(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_tenders_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_orders_with_bids_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_start_price_bid_orders_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_start_price_bid_accepted_orders_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_start_price_bid_rides_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_accepted_orders_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_rides_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(TEST_rides_count_by_bids(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_price_start_usd_sum(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_rides_price_start_usd_sum(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_orders_without_bids_price_start_usd_sum(df_order_with_recprice, group_cols), on=group_cols, how='left')           
           .merge(metric_price_highrate_usd_sum(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_rides_price_highrate_usd_sum(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_orders_without_bids_price_highrate_usd_sum(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_price_tender_usd_sum(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_price_done_usd_sum(df_order_with_recprice, group_cols), on=group_cols, how='left')
           #
           .merge(metric_good_orders_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_price_base_usd_sum(df_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_recprice_usd_sum(df_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_minprice_usd_sum(df_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_sum(df_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_gr_1_sum(df_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_gr_1_calcprices_count(df_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_le_1_sum(df_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_le_1_calcprices_count(df_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_orders_by_minprice_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_orders_by_minprice_with_bids_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_accepted_orders_by_minprice_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_rides_by_minprice_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_gr_1_orders_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_le_1_orders_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_gr_1_orders_with_bids_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_le_1_orders_with_bids_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_gr_1_start_price_bid_orders_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_le_1_start_price_bid_orders_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_gr_1_accepted_orders_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_le_1_accepted_orders_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_gr_1_rides_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_le_1_rides_count(df_order_with_recprice, group_cols), on=group_cols, how='left')
           #
           .merge(metric_rides_by_bid_option_startprice_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_bids_option_startprice_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_bids_option_startprice_bid_price_currency_sum(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_accepted_bids_option_startprice_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_bids_option_startprice_accepted_bid_price_currency_sum(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_rides_by_bid_option_option1_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_bids_option_option1_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_bids_option_option1_bid_price_currency_sum(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_accepted_bids_option_option1_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_accepted_bids_option_option1_bid_price_currency_sum(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_rides_by_bid_option_option2_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_bids_option_option2_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_bids_option_option2_bid_price_currency_sum(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_accepted_bids_option_option2_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_accepted_bids_option_option2_bid_price_currency_sum(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_rides_by_bid_option_option3_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_bids_option_option3_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_bids_option_option3_bid_price_currency_sum(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_accepted_bids_option_option3_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_accepted_bids_option_option3_bid_price_currency_sum(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_rides_by_bid_option_other1_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_bids_option_other1_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_bids_option_other1_bid_price_currency_sum(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_accepted_bids_option_other1_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_accepted_bids_option_other1_bid_price_currency_sum(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_rides_by_bid_option_other2_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_bids_option_other2_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_bids_option_other2_bid_price_currency_sum(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_accepted_bids_option_other2_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_accepted_bids_option_other2_bid_price_currency_sum(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_rides_by_bid_option_other3_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_bids_option_other3_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_bids_option_other3_bid_price_currency_sum(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_accepted_bids_option_other3_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_accepted_bids_option_other3_bid_price_currency_sum(df_bid, group_cols), on=group_cols, how='left')
           #
           .merge(metric_orders_with_bid_option_startprice_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_orders_with_bid_option_option1_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_orders_with_bid_option_option2_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_orders_with_bid_option_option3_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_orders_with_bid_option_other1_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_orders_with_bid_option_other2_count(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_orders_with_bid_option_other3_count(df_bid, group_cols), on=group_cols, how='left')
           #
           .merge(metric_time_to_1st_bid_sec(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_time_1st_bid_to_accept_sec(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_rta_sum(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_rtr_sum(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_eta_sum(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_etr_sum(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_eta_done_orders_sum(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_eta_accepted_bids_sum(df_bid, group_cols), on=group_cols, how='left')
           .merge(metric_etr_orders_with_bids_sum(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_etr_orders_with_accepted_bids_sum(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_etr_done_orders_sum(df_order_with_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_etr_orders_without_bids_sum(df_order_with_recprice, group_cols), on=group_cols, how='left'))
    return dfm

           
def calculate_ratio_metrics(df, metric_list=METRIC_LIST):
    for metric, num, den in metric_list:
        try:
            df[metric] = df[num] / df[den]
        except:
            df[metric] = np.nan
    return df