import warnings
warnings.filterwarnings("ignore")

import pathlib
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.offline as pyo

import h3
import geopandas as gpd
from shapely.geometry import Polygon, Point, MultiPoint


def plot_switches_matrix(df_exp, plot_root_path, is_show=True):
    
    pyo.init_notebook_mode()
    
    gb = (df_exp
          .groupby(['weekday_name', 'hour', 'group_name'])
          .size()
          .rename('switches_count')
          .reset_index()
          .pivot(index=['weekday_name', 'hour'], columns='group_name', values='switches_count')
          .reset_index())
    gb['abs_diff'] = gb['A'] - gb['Control']
    gb = gb.pivot(index='hour', columns='weekday_name', values='abs_diff')
    gb = gb[['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']]
    
    fig = px.imshow(gb, labels=dict(x="Day of Week", y="Hour of Day", color='A - Control'))
    fig.write_html(plot_root_path / 'switches_matrix.html')
    
    if is_show:
        fig.show()


def plot_conversions_by_time(df_grouped, grouped_column, plot_root_path, is_before=True, is_show=True):

    pyo.init_notebook_mode()

    fig = make_subplots(rows=3, cols=1, 
                        row_titles=['absolutes', 'main conversions', 'bid conversions'])

    # -------------------------------------------------------------------------
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Control']['calcprices_count'],
                             mode='lines',
                             name='calcprices (Control)',
                             line=dict(color='#AB63FA', width=2, dash='dash')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'A']['calcprices_count'],
                             mode='lines',
                             name='calcprices (A)',
                             line=dict(color='#750D86', width=2, dash='solid')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Control']['orders_count'],
                             mode='lines',
                             name='orders (Control)',
                             line=dict(color='#2CA02C', width=2, dash='dash')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'A']['orders_count'],
                             mode='lines',
                             name='orders (A)',
                             line=dict(color='#1C8356', width=2, dash='solid')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Control']['rides_count'],
                             mode='lines',
                             name='rides (Control)',
                             line=dict(color='#D62728', width=2, dash='dash')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'A']['rides_count'],
                             mode='lines',
                             name='rides (A)',
                             line=dict(color='#AF0038', width=2, dash='solid')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Control']['accepted_orders_count'],
                             mode='lines',
                             name='accepted_orders (Control)',
                             line=dict(color='#FF7F0E', width=2, dash='dash')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'A']['accepted_orders_count'],
                             mode='lines',
                             name='accepted_orders (A)',
                             line=dict(color='#EB663B', width=2, dash='solid')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Control']['orders_with_bids_count'],
                             mode='lines',
                             name='orders_with_bids (Control)',
                             line=dict(color='#3366CC', width=2, dash='dash')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'A']['orders_with_bids_count'],
                             mode='lines',
                             name='orders_with_bids (A)',
                             line=dict(color='#325A9B', width=2, dash='solid')),
                  row=1, col=1)
    # -------------------------------------------------------------------------
    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Before']['cp2order'],
                             mode='lines',
                             name='cp2order (Before)',
                             line=dict(color='#2CA02C', width=2, dash='dot')),
                  row=2, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Control']['cp2order'],
                             mode='lines',
                             name='cp2order (Control)',
                             line=dict(color='#2CA02C', width=2, dash='dash')),
                  row=2, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'A']['cp2order'],
                             mode='lines',
                             name='cp2order (A)',
                             line=dict(color='#1C8356', width=2, dash='solid')),
                  row=2, col=1)
    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Before']['order2done'],
                             mode='lines',
                             name='order2done (Before)',
                             line=dict(color='#D62728', width=2, dash='dot')),
                  row=2, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Control']['order2done'],
                             mode='lines',
                             name='order2done (Control)',
                             line=dict(color='#D62728', width=2, dash='dash')),
                  row=2, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'A']['order2done'],
                             mode='lines',
                             name='order2done (A)',
                             line=dict(color='#AF0038', width=2, dash='solid')),
                  row=2, col=1)
    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Before']['cp2done'],
                             mode='lines',
                             name='cp2done (Before)',
                             line=dict(color='#AB63FA', width=2, dash='dot')),
                  row=2, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Control']['cp2done'],
                             mode='lines',
                             name='cp2done (Control)',
                             line=dict(color='#AB63FA', width=2, dash='dash')),
                  row=2, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'A']['cp2done'],
                             mode='lines',
                             name='cp2done (A)',
                             line=dict(color='#750D86', width=2, dash='solid')),
                  row=2, col=1)
    # -------------------------------------------------------------------------
    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Before']['order2bid'],
                             mode='lines',
                             name='order2bid (Before)',
                             line=dict(color='#3366CC', width=2, dash='dot')),
                  row=3, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Control']['order2bid'],
                             mode='lines',
                             name='order2bid (Control)',
                             line=dict(color='#3366CC', width=2, dash='dash')),
                  row=3, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'A']['order2bid'],
                             mode='lines',
                             name='order2bid (A)',
                             line=dict(color='#325A9B', width=2, dash='solid')),
                  row=3, col=1)
    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Before']['bid2accept'],
                             mode='lines',
                             name='bid2accept (Before)',
                             line=dict(color='#2CA02C', width=2, dash='dot')),
                  row=3, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Control']['bid2accept'],
                             mode='lines',
                             name='bid2accept (Control)',
                             line=dict(color='#2CA02C', width=2, dash='dash')),
                  row=3, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'A']['bid2accept'],
                             mode='lines',
                             name='bid2accept (A)',
                             line=dict(color='#1C8356', width=2, dash='solid')),
                  row=3, col=1)
    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Before']['order2accept'],
                             mode='lines',
                             name='order2accept (Before)',
                             line=dict(color='#FF7F0E', width=2, dash='dot')),
                  row=3, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Control']['order2accept'],
                             mode='lines',
                             name='order2accept (Control)',
                             line=dict(color='#FF7F0E', width=2, dash='dash')),
                  row=3, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'A']['order2accept'],
                             mode='lines',
                             name='order2accept (A)',
                             line=dict(color='#EB663B', width=2, dash='solid')),
                  row=3, col=1)

    fig.update_layout(
        title="Conversion Dynamics",
        legend_title="Metric",
        autosize=False,
        width=1000,
        height=1500,
    )
    fig.write_html(plot_root_path / f'conversions_by_{grouped_column}.html')
    
    if is_show:
        fig.show()
        
        
def plot_prices_by_time(df_grouped, grouped_column, plot_root_path, is_before=True, is_show=True):

    pyo.init_notebook_mode()

    fig = make_subplots(rows=1, cols=1, 
                        row_titles=['prices, usd'])
    
    # -------------------------------------------------------------------------
    # if is_before:
    #     fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column], 
    #                          y=df_grouped[df_grouped.group_name == 'Before']['minprice_usd'],
    #                          mode='lines',
    #                          name='minprice_usd (Before)',
    #                          line=dict(color='#7F7F7F', width=2, dash='dot')),
    #               row=1, col=1)
    # fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
    #                          y=df_grouped[df_grouped.group_name == 'Control']['minprice_usd'],
    #                          mode='lines',
    #                          name='minprice_usd (Control)',
    #                          line=dict(color='#7F7F7F', width=2, dash='dash')),
    #               row=1, col=1)
    # fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
    #                          y=df_grouped[df_grouped.group_name == 'A']['minprice_usd'],
    #                          mode='lines',
    #                          name='minprice_usd (A)',
    #                          line=dict(color='#565656', width=2, dash='solid')),
    #               row=1, col=1)
    # if is_before:
    #     fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column], 
    #                          y=df_grouped[df_grouped.group_name == 'Before']['price_base_usd'],
    #                          mode='lines',
    #                          name='price_base_usd (Before)',
    #                          line=dict(color='#E377C2', width=2, dash='dot')),
    #               row=1, col=1)
    # fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
    #                          y=df_grouped[df_grouped.group_name == 'Control']['price_base_usd'],
    #                          mode='lines',
    #                          name='price_base_usd (Control)',
    #                          line=dict(color='#E377C2', width=2, dash='dash')),
    #               row=1, col=1)
    # fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
    #                          y=df_grouped[df_grouped.group_name == 'A']['price_base_usd'],
    #                          mode='lines',
    #                          name='price_base_usd (A)',
    #                          line=dict(color='#FA0087', width=2, dash='solid')),
    #               row=1, col=1)
    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Before']['recprice_usd'],
                             mode='lines',
                             name='recprice_usd (Before)',
                             line=dict(color='#AB63FA', width=2, dash='dot')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Control']['recprice_usd'],
                             mode='lines',
                             name='recprice_usd (Control)',
                             line=dict(color='#AB63FA', width=2, dash='dash')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'A']['recprice_usd'],
                             mode='lines',
                             name='recprice_usd (A)',
                             line=dict(color='#750D86', width=2, dash='solid')),
                  row=1, col=1)
    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Before']['price_highrate_usd'],
                             mode='lines',
                             name='price_highrate_usd (Before)',
                             line=dict(color='#3366CC', width=2, dash='dot')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Control']['price_highrate_usd'],
                             mode='lines',
                             name='price_highrate_usd (Control)',
                             line=dict(color='#3366CC', width=2, dash='dash')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'A']['price_highrate_usd'],
                             mode='lines',
                             name='price_highrate_usd (A)',
                             line=dict(color='#325A9B', width=2, dash='solid')),
                  row=1, col=1)
    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Before']['price_highrate_usd_rides'],
                             mode='lines',
                             name='price_highrate_usd_rides (Before)',
                             line=dict(color='#2CA02C', width=2, dash='dot')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Control']['price_highrate_usd_rides'],
                             mode='lines',
                             name='price_highrate_usd_rides (Control)',
                             line=dict(color='#2CA02C', width=2, dash='dash')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'A']['price_highrate_usd_rides'],
                             mode='lines',
                             name='price_highrate_usd_rides (A)',
                             line=dict(color='#1C8356', width=2, dash='solid')),
                  row=1, col=1)
    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Before']['price_start_usd'],
                             mode='lines',
                             name='price_start_usd (Before)',
                             line=dict(color='#FF7F0E', width=2, dash='dot')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Control']['price_start_usd'],
                             mode='lines',
                             name='price_start_usd (Control)',
                             line=dict(color='#FF7F0E', width=2, dash='dash')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'A']['price_start_usd'],
                             mode='lines',
                             name='price_start_usd (A)',
                             line=dict(color='#EB663B', width=2, dash='solid')),
                  row=1, col=1)
    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Before']['price_start_usd_rides'],
                             mode='lines',
                             name='price_start_usd_rides (Before)',
                             line=dict(color='#EECA3B', width=2, dash='dot')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Control']['price_start_usd_rides'],
                             mode='lines',
                             name='price_start_usd_rides (Control)',
                             line=dict(color='#EECA3B', width=2, dash='dash')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'A']['price_start_usd_rides'],
                             mode='lines',
                             name='price_start_usd_rides (A)',
                             line=dict(color='#FEAF16', width=2, dash='solid')),
                  row=1, col=1)
    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Before']['price_tender_usd'],
                             mode='lines',
                             name='price_tender_usd (Before)',
                             line=dict(color='#19D3F3', width=2, dash='dot')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Control']['price_tender_usd'],
                             mode='lines',
                             name='price_tender_usd (Control)',
                             line=dict(color='#19D3F3', width=2, dash='dash')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'A']['price_tender_usd'],
                             mode='lines',
                             name='price_tender_usd (A)',
                             line=dict(color='#17BECF', width=2, dash='solid')),
                  row=1, col=1)
    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Before']['price_done_usd'],
                             mode='lines',
                             name='price_done_usd (Before)',
                             line=dict(color='#D62728', width=2, dash='dot')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'Control']['price_done_usd'],
                             mode='lines',
                             name='price_done_usd (Control)',
                             line=dict(color='#D62728', width=2, dash='dash')),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
                             y=df_grouped[df_grouped.group_name == 'A']['price_done_usd'],
                             mode='lines',
                             name='price_done_usd (A)',
                             line=dict(color='#AF0038', width=2, dash='solid')),
                  row=1, col=1)
    # -------------------------------------------------------------------------
    # if is_before:
    #     fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column], 
    #                          y=df_grouped[df_grouped.group_name == 'Before']['surge'],
    #                          mode='lines',
    #                          name='surge (Before)',
    #                          line=dict(color='#FF7F0E', width=2, dash='dot')),
    #               row=2, col=1)
    # fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column], 
    #                          y=df_grouped[df_grouped.group_name == 'Control']['surge'],
    #                          mode='lines',
    #                          name='surge (Control)',
    #                          line=dict(color='#FF7F0E', width=2, dash='dash')),
    #               row=2, col=1)
    # fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column], 
    #                          y=df_grouped[df_grouped.group_name == 'A']['surge'],
    #                          mode='lines',
    #                          name='surge (A)',
    #                          line=dict(color='#EB663B', width=2, dash='solid')),
    #               row=2, col=1)

    fig.update_layout(
        title="Prices Dynamics",
        legend_title="Metric",
        autosize=False,
        width=1000,
        height=1000,
    )
    fig.write_html(plot_root_path / f'prices_by_{grouped_column}.html')
    
    if is_show:
        fig.show()
        
        
def plot_metric_by_time(df_grouped, grouped_column, metric_name, plot_root_path, is_before=True, is_show=True):
    
    pyo.init_notebook_mode()
    
    if is_before:
        df_before = pd.merge(
            df_grouped[grouped_column], 
            df_grouped[df_grouped.group_name == 'Before'], 
            how='left', 
            on=grouped_column
        ).sort_values(by=grouped_column)
    
    df_control = pd.merge(
        df_grouped[grouped_column], 
        df_grouped[df_grouped.group_name == 'Control'], 
        how='left', 
        on=grouped_column
    ).sort_values(by=grouped_column)

    df_a = pd.merge(
        df_grouped[grouped_column], 
        df_grouped[df_grouped.group_name == 'A'], 
        how='left', 
        on=grouped_column
    ).sort_values(by=grouped_column)

    fig = go.Figure()
    
    if is_before:
        fig.add_trace(go.Scatter(x=df_before[grouped_column], y=df_before[metric_name], name='Before'))
    fig.add_trace(go.Scatter(x=df_a[grouped_column], y=df_a[metric_name], name='A'))
    fig.add_trace(go.Scatter(x=df_control[grouped_column], y=df_control[metric_name], name='Control'))

    fig.update_layout(title=metric_name)
    fig.update_traces(mode='lines+markers')
    
    fig.write_html(plot_root_path / f'{metric_name}_by_{grouped_column}.html')
    
    if is_show:
        fig.show()
        
        
def plot_metric_by_hex(
    df_grouped, 
    group_name, 
    metric_name, 
    hex_size, 
    lat_center, 
    lon_center, 
    plot_root_path,
    is_show=True,
):
    
    def add_geometry(hexx):
        points = h3.h3_to_geo_boundary(hexx, True)
        return Polygon(points)
    
    pyo.init_notebook_mode()
    
    df_plot = df_grouped[df_grouped.group_name == group_name]
    df_plot['polygon'] = df_plot[f'hex_from_calc_{hex_size}'].apply(lambda x: add_geometry(x))
    df_plot_gpd = gpd.GeoDataFrame(df_plot, geometry='polygon').set_index(f'hex_from_calc_{hex_size}')
    
    fig = px.choropleth_mapbox(
        df_plot_gpd,
        geojson=df_plot_gpd.polygon,
        locations=df_plot_gpd.index,
        mapbox_style="open-street-map",
        color=df_plot_gpd[metric_name],
        center={"lat": lat_center, "lon": lon_center},
        opacity=0.5,
        hover_data=[metric_name],
        width=1000,
        height=1000,
        color_continuous_scale="Viridis",
        zoom=9,
    )
    
    fig.write_html(plot_root_path / f'{metric_name}_by_hexagon.html')
    
    if is_show:
        fig.show()

def plot_times_by_time(df_grouped, grouped_column, plot_root_path, is_before=True, is_show=True):
    """
    Plot time-related metrics over time.
    
    Parameters:
    -----------
    df_grouped : pandas.DataFrame
        Grouped dataframe with metrics
    grouped_column : str
        Column name for x-axis (usually 'time')
    plot_root_path : pathlib.Path
        Path to save the plot
    is_before : bool, optional
        Whether to include 'Before' group in the plot
    is_show : bool, optional
        Whether to display the plot
    """
    pyo.init_notebook_mode()

    fig = make_subplots(rows=3, cols=1,
                       row_titles=['ETA metrics', 'ETR metrics', 'Time to bid metrics'])

    # -------------------------------------------------------------------------
    # First subplot: ETA metrics
    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column],
                                y=df_grouped[df_grouped.group_name == 'Before']['eta'],
                                mode='lines',
                                name='eta (Before)',
                                line=dict(color='#2CA02C', width=2, dash='dot')),
                     row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column],
                            y=df_grouped[df_grouped.group_name == 'Control']['eta'],
                            mode='lines',
                            name='eta (Control)',
                            line=dict(color='#2CA02C', width=2, dash='dash')),
                 row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column],
                            y=df_grouped[df_grouped.group_name == 'A']['eta'],
                            mode='lines',
                            name='eta (A)',
                            line=dict(color='#1C8356', width=2, dash='solid')),
                 row=1, col=1)

    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column],
                                y=df_grouped[df_grouped.group_name == 'Before']['eta_done_orders'],
                                mode='lines',
                                name='eta_done_orders (Before)',
                                line=dict(color='#D62728', width=2, dash='dot')),
                     row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column],
                            y=df_grouped[df_grouped.group_name == 'Control']['eta_done_orders'],
                            mode='lines',
                            name='eta_done_orders (Control)',
                            line=dict(color='#D62728', width=2, dash='dash')),
                 row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column],
                            y=df_grouped[df_grouped.group_name == 'A']['eta_done_orders'],
                            mode='lines',
                            name='eta_done_orders (A)',
                            line=dict(color='#AF0038', width=2, dash='solid')),
                 row=1, col=1)

    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column],
                                y=df_grouped[df_grouped.group_name == 'Before']['eta_accepted_bids'],
                                mode='lines',
                                name='eta_accepted_bids (Before)',
                                line=dict(color='#AB63FA', width=2, dash='dot')),
                     row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column],
                            y=df_grouped[df_grouped.group_name == 'Control']['eta_accepted_bids'],
                            mode='lines',
                            name='eta_accepted_bids (Control)',
                            line=dict(color='#AB63FA', width=2, dash='dash')),
                 row=1, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column],
                            y=df_grouped[df_grouped.group_name == 'A']['eta_accepted_bids'],
                            mode='lines',
                            name='eta_accepted_bids (A)',
                            line=dict(color='#750D86', width=2, dash='solid')),
                 row=1, col=1)

    # -------------------------------------------------------------------------
    # Second subplot: ETR metrics
    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column],
                                y=df_grouped[df_grouped.group_name == 'Before']['etr'],
                                mode='lines',
                                name='etr (Before)',
                                line=dict(color='#2CA02C', width=2, dash='dot')),
                     row=2, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column],
                            y=df_grouped[df_grouped.group_name == 'Control']['etr'],
                            mode='lines',
                            name='etr (Control)',
                            line=dict(color='#2CA02C', width=2, dash='dash')),
                 row=2, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column],
                            y=df_grouped[df_grouped.group_name == 'A']['etr'],
                            mode='lines',
                            name='etr (A)',
                            line=dict(color='#1C8356', width=2, dash='solid')),
                 row=2, col=1)

    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column],
                                y=df_grouped[df_grouped.group_name == 'Before']['etr_done_orders'],
                                mode='lines',
                                name='etr_done_orders (Before)',
                                line=dict(color='#D62728', width=2, dash='dot')),
                     row=2, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column],
                            y=df_grouped[df_grouped.group_name == 'Control']['etr_done_orders'],
                            mode='lines',
                            name='etr_done_orders (Control)',
                            line=dict(color='#D62728', width=2, dash='dash')),
                 row=2, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column],
                            y=df_grouped[df_grouped.group_name == 'A']['etr_done_orders'],
                            mode='lines',
                            name='etr_done_orders (A)',
                            line=dict(color='#AF0038', width=2, dash='solid')),
                 row=2, col=1)

    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column],
                                y=df_grouped[df_grouped.group_name == 'Before']['etr_orders_with_bids'],
                                mode='lines',
                                name='etr_orders_with_bids (Before)',
                                line=dict(color='#AB63FA', width=2, dash='dot')),
                     row=2, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column],
                            y=df_grouped[df_grouped.group_name == 'Control']['etr_orders_with_bids'],
                            mode='lines',
                            name='etr_orders_with_bids (Control)',
                            line=dict(color='#AB63FA', width=2, dash='dash')),
                 row=2, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column],
                            y=df_grouped[df_grouped.group_name == 'A']['etr_orders_with_bids'],
                            mode='lines',
                            name='etr_orders_with_bids (A)',
                            line=dict(color='#750D86', width=2, dash='solid')),
                 row=2, col=1)

    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column],
                                y=df_grouped[df_grouped.group_name == 'Before']['etr_orders_without_bids'],
                                mode='lines',
                                name='etr_orders_without_bids (Before)',
                                line=dict(color='#FF7F0E', width=2, dash='dot')),
                     row=2, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column],
                            y=df_grouped[df_grouped.group_name == 'Control']['etr_orders_without_bids'],
                            mode='lines',
                            name='etr_orders_without_bids (Control)',
                            line=dict(color='#FF7F0E', width=2, dash='dash')),
                 row=2, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column],
                            y=df_grouped[df_grouped.group_name == 'A']['etr_orders_without_bids'],
                            mode='lines',
                            name='etr_orders_without_bids (A)',
                            line=dict(color='#EB663B', width=2, dash='solid')),
                 row=2, col=1)

    # -------------------------------------------------------------------------
    # Third subplot: Time to bid metrics
    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column],
                                y=df_grouped[df_grouped.group_name == 'Before']['time_to_1st_bid_sec'],
                                mode='lines',
                                name='time_to_1st_bid_sec (Before)',
                                line=dict(color='#2CA02C', width=2, dash='dot')),
                     row=3, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column],
                            y=df_grouped[df_grouped.group_name == 'Control']['time_to_1st_bid_sec'],
                            mode='lines',
                            name='time_to_1st_bid_sec (Control)',
                            line=dict(color='#2CA02C', width=2, dash='dash')),
                 row=3, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column],
                            y=df_grouped[df_grouped.group_name == 'A']['time_to_1st_bid_sec'],
                            mode='lines',
                            name='time_to_1st_bid_sec (A)',
                            line=dict(color='#1C8356', width=2, dash='solid')),
                 row=3, col=1)

    if is_before:
        fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Before'][grouped_column],
                                y=df_grouped[df_grouped.group_name == 'Before']['time_1st_bid_to_accept_sec'],
                                mode='lines',
                                name='time_1st_bid_to_accept_sec (Before)',
                                line=dict(color='#D62728', width=2, dash='dot')),
                     row=3, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'Control'][grouped_column],
                            y=df_grouped[df_grouped.group_name == 'Control']['time_1st_bid_to_accept_sec'],
                            mode='lines',
                            name='time_1st_bid_to_accept_sec (Control)',
                            line=dict(color='#D62728', width=2, dash='dash')),
                 row=3, col=1)
    fig.add_trace(go.Scatter(x=df_grouped[df_grouped.group_name == 'A'][grouped_column],
                            y=df_grouped[df_grouped.group_name == 'A']['time_1st_bid_to_accept_sec'],
                            mode='lines',
                            name='time_1st_bid_to_accept_sec (A)',
                            line=dict(color='#AF0038', width=2, dash='solid')),
                 row=3, col=1)

    # Update layout
    fig.update_layout(
        title="Time Metrics Dynamics",
        legend_title="Metric",
        autosize=False,
        width=1000,
        height=1500,
    )

    # Save and show plot
    fig.write_html(plot_root_path / f'times_by_{grouped_column}.html')
    
    if is_show:
        fig.show()

def plot_algo_heatmap(df_bids, min_samples=10):
    """
    Создает тепловую карту, где цвет ячейки отражает долю строк с 'algo_name_new' == 'algo_bid_mph' 
    в тестовой группе 'A'.
    
    Параметры:
    df_bids (DataFrame): DataFrame с данными о ставках
    min_samples (int): Минимальное количество записей в бине для его отображения
    """
    # Проверяем наличие необходимых колонок
    required_cols = ['algo_name_new', 'group_name', 'eta', 'duration_in_min']
    for col in required_cols:
        if col not in df_bids.columns:
            raise ValueError(f"Колонка {col} отсутствует в df_bids")
    
    # Фильтруем только тестовую группу 'A'
    df_test = df_bids[df_bids['group_name'] == 'A']
    
    if len(df_test) == 0:
        print("Нет данных для тестовой группы 'A'")
        return
    
    # Создаем бины для eta и duration_in_min
    # Конвертируем duration_in_min в секунды для сопоставимости с eta
    df_test['duration_sec'] = df_test['duration_in_min'] * 60
    
    # Определяем максимальные значения для бинов
    max_eta = int(np.ceil(df_test['eta'].max() / 60.0)) * 60
    max_duration = int(np.ceil(df_test['duration_sec'].max() / 60.0)) * 60
    
    # Создаем бины по 60 секунд
    df_test['eta_bin'] = pd.cut(df_test['eta'], 
                              bins=np.arange(0, max_eta + 120, 60), 
                              labels=[f"{i}-{i+60}" for i in range(0, max_eta + 60, 60)])
    
    df_test['duration_bin'] = pd.cut(df_test['duration_sec'], 
                                    bins=np.arange(0, max_duration + 120, 60), 
                                    labels=[f"{i}-{i+60}" for i in range(0, max_duration + 60, 60)])
    
    for geo in df_test['city_id'].unique():
        for type_name in df_test['order_type'].unique():
            # Фильтруем данные
            df_geo_type = df_test[(df_test['city_id'] == geo) & 
                                 (df_test['order_type'] == type_name)]
            
            if len(df_geo_type) == 0:
                continue
            
            # Подсчитываем общее количество записей в каждом бине
            bin_counts = df_geo_type.groupby(['eta_bin', 'duration_bin']).size().reset_index(name='count')
            
            # Подсчитываем долю записей с algo_name_new == 'algo_bid_mph' для каждой комбинации eta_bin и duration_bin
            algo_counts = df_geo_type[df_geo_type['algo_name_new'] == 'algo_bid_mph'].groupby(['eta_bin', 'duration_bin']).size().reset_index(name='algo_count')
            
            # Объединяем данные
            merged = pd.merge(bin_counts, algo_counts, on=['eta_bin', 'duration_bin'], how='left')
            merged['algo_count'] = merged['algo_count'].fillna(0)
            merged['algo_bid_mph_share'] = merged['algo_count'] / merged['count']
            
            # Фильтруем бины с недостаточным количеством записей
            filtered_bins = merged[merged['count'] >= min_samples]
            
            if len(filtered_bins) == 0:
                print(f"Нет бинов с количеством записей >= {min_samples} для {geo}, {type_name}")
                continue
            
            # Преобразуем в pivot для тепловой карты
            pivot_table = filtered_bins.pivot(index='duration_bin', columns='eta_bin', values='algo_bid_mph_share')
            
            # Создаем тепловую карту
            fig = go.Figure()
            
            heatmap = go.Heatmap(
                z=pivot_table.values,
                x=pivot_table.columns,
                y=pivot_table.index,
                colorscale='RdBu',
                zmid=0.5,  # Устанавливаем среднюю точку для цветовой шкалы
                colorbar=dict(title="Доля algo_bid_mph", len=0.8)
            )
            
            fig.add_trace(heatmap)
            
            # Настраиваем макет
            fig.update_layout(
                title_text=f"{geo}, {type_name} - Доля algo_bid_mph (min_samples={min_samples})",
                width=900,
                height=700,
                template='plotly_white',
                xaxis_title="ETA (seconds)",
                yaxis_title="Duration (seconds)"
            )
            
            # Показываем график
            fig.show()

def plot_switches_matrix(df_exp, plot_root_path, is_show=True):
    
    pyo.init_notebook_mode()
    
    gb = (df_exp
          .groupby(['weekday_name', 'hour', 'group_name'])
          .size()
          .rename('switches_count')
          .reset_index()
          .pivot(index=['weekday_name', 'hour'], columns='group_name', values='switches_count')
          .reset_index())
    gb['abs_diff'] = gb['A'] - gb['Control']
    gb = gb.pivot(index='hour', columns='weekday_name', values='abs_diff')
    gb = gb[['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']]
    
    fig = px.imshow(gb, labels=dict(x="Day of Week", y="Hour of Day", color='A - Control'))
    fig.write_html(plot_root_path / 'switches_matrix.html')
    
    if is_show:
        fig.show()