import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import os
from typing import Callable, List, Optional, Tuple

# Функция для числителя
def calc_algo_mph(df, group_cols):
    result_df = (
        df[df['algo_name_new'] == 'algo_bidmph']
        .groupby(group_cols)
        .size()
        .reset_index(name="algo_count_value")
    )
    return result_df

# Функция для знаменателя
def calc_total(df, group_cols):
    result_df = (
        df.groupby(group_cols)
        .size()
        .reset_index(name="total_count")
    )
    return result_df

class RatioBinnedComputer:
    def __init__(
        self,
        df: pd.DataFrame,
        group_cols: List[str],
        numerator_func: Callable[[pd.DataFrame, List[str]], Tuple[pd.DataFrame, str]],
        denominator_func: Callable[[pd.DataFrame, List[str]], Tuple[pd.DataFrame, str]],
        # num_col: str,
        # denom_col: str,
        ratio_col: str,
        min_samples: int = 1,
        denom_extra_arg=None,  # Add parameter for optional denominator value
    ):
        self.df = df
        self.group_cols = group_cols
        self.numerator_func = numerator_func
        self.denominator_func = denominator_func
        # self.num_col = num_col
        # self.denom_col = denom_col
        self.ratio_col = ratio_col
        self.min_samples = min_samples
        self.num_col = None  # Will be set during compute
        self.denom_col = None  # Will be set during compute
        self.denom_extra_arg = denom_extra_arg  # Store the extra denominator value

    def compute(self) -> pd.DataFrame:
        # Вычисляем числитель
        numer_df = self.numerator_func(self.df, self.group_cols)
        self.num_col = list(set(numer_df.columns) - set(self.group_cols))[0]

        # Если передан denom_extra_arg, используем его напрямую
        if self.denom_extra_arg is not None:
            # Создаем копию таблицы числителя и добавляем колонку с фиксированным знаменателем
            result_df = numer_df.copy()
            self.denom_col = "denom"
            result_df[self.denom_col] = self.denom_extra_arg
            result_df[self.ratio_col] = result_df[self.num_col] / result_df[self.denom_col]
            return result_df
        else:
            # Стандартный flow с вычислением знаменателя
            denom_df = self.denominator_func(self.df, self.group_cols)
            self.denom_col = list(set(denom_df.columns) - set(self.group_cols))[0]

            # Объединяем
            merged = pd.merge(denom_df, numer_df, on=self.group_cols, how="left")
            merged[self.num_col] = merged[self.num_col].fillna(0)
            merged[self.ratio_col] = merged[self.num_col] / merged[self.denom_col]

            return merged

    def to_pivot(
        self,
        df: Optional[pd.DataFrame] = None,
        index: str = None,
        columns: str = None,
        values: str = None
    ) -> pd.DataFrame:
        df = df if df is not None else self.compute()
        if not index or not columns:
            raise ValueError("`index` and `columns` must be specified for pivot.")
        return df.pivot(index=index, columns=columns, values=values)
    
def create_duration_bin(df, reference_df, num_bins=30):
    # Convert minutes to seconds in both dataframes if needed
    df['duration_sec'] = df['duration_in_min'] * 60
    
    if 'duration_sec' not in reference_df.columns:
        reference_df = reference_df.copy()
        reference_df['duration_sec'] = reference_df['duration_in_min'] * 60
    
    # Calculate bin edges from reference dataframe
    bin_edges = [reference_df['duration_sec'].min()]
    bin_edges.extend([
        reference_df['duration_sec'].quantile(q) 
        for q in np.linspace(0, 1, num_bins+1)[1:]
    ])
    bin_edges = sorted(list(set(bin_edges)))  # Remove duplicates and sort
    # print(bin_edges)
    # Create labels based on percentile ranges
    labels = bin_edges[:-1]
    
    # Apply these bins to the target dataframe
    df['duration_bin'] = pd.cut(
        df['duration_sec'],
        bins=bin_edges,
        labels=labels,
        right=False
    )
    return df

def plot_heatmap(df, metric, numerator_func=calc_algo_mph, denominator_func=calc_total, groups=['Control', 'A'],
                 min_samples=10, zmid1=0.0, zmin1=None, zmax1=None,  
                 zmid2=0.0, zmin2=None, zmax2=None,
                 PLOT_ROOT_PATH=None, EXP_ID=None, 
                 denom_values_dict=None, num_bins=20):
    # Проверяем наличие необходимых колонок
    required_cols = ['group_name', 'eta', 'duration_in_min']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Колонка {col} отсутствует в данных")
        
    # Определяем максимальные значения для бинов
    df['duration_sec'] = df['duration_in_min'] * 60
    eta_percentile = df['eta'].quantile(0.99)
    # eta_max = int(np.ceil(eta_percentile / 60.0)) * 60
    eta_max = 600
    # duration_percentile = df['duration_sec'].quantile(0.99)
    # duration_max = int(np.ceil(duration_percentile / 60.0)) * 60 
    # max_eta = int(np.ceil(df['eta'].max() / 60.0)) * 60
    # max_duration = int(np.ceil(df['duration_sec'].max() / 60.0)) * 60

    # Создаем бины для eta и duration_in_min
    df['eta_bin'] = pd.cut(
         df['eta'],
         bins=np.arange(0, eta_max + 60, 60),
         labels=[f"{i}" for i in range(0, eta_max, 60)],
         right=False
         )

    # df['duration_bin'] = pd.cut(
    #     df['duration_sec'],
    #     bins=np.arange(0, duration_max, 60*5),
    #     labels=[f"{i}" for i in range(0, duration_max - 60*5, 60*5)],
    #     right=False
    #     )
    df = create_duration_bin(df, df, num_bins=num_bins)
    
    # Create the output directory if PLOT_ROOT_PATH is provided
    if PLOT_ROOT_PATH:
        os.makedirs(f"{PLOT_ROOT_PATH}/heatmaps", exist_ok=True)

    for geo in df['city_id'].unique():
        for type_name in df['order_type'].unique():
            pivot_tables = {}
            for group in groups:   
                df_geo_type_group = df[
                    (df['city_id'] == geo) &
                    (df['order_type'] == type_name) &
                    (df['group_name'] == group)
                ].copy()

                if df_geo_type_group.empty:
                    print(f"Нет данных для группы {group} ({geo}, {type_name})")
                    continue

                # Определяем denom_extra_arg для конкретной группы, если словарь предоставлен
                current_denom_value = None
                if denom_values_dict is not None and group in denom_values_dict:
                    current_denom_value = denom_values_dict[group]
                
                # Используем класс RatioBinnedComputer
                computer = RatioBinnedComputer(
                    df=df_geo_type_group,
                    group_cols=['duration_bin', 'eta_bin'],
                    numerator_func=numerator_func,
                    denominator_func=denominator_func,
                    ratio_col=metric,
                    min_samples=min_samples,
                    denom_extra_arg=current_denom_value  # Pass the group-specific denominator value
                )

                heatmap_df = computer.compute()

                if heatmap_df.empty:
                    print(f"Нет бинов с количеством записей >= {min_samples} для {geo}, {type_name}, {group}")
                    continue

                # Prepare data for pivot table with hover information
                pivot_table = computer.to_pivot(
                    index='eta_bin', 
                    columns='duration_bin',
                    values=computer.ratio_col
                )
                
                # Create pivot tables for numerator and denominator
                pivot_num = heatmap_df.pivot(
                    index='eta_bin', 
                    columns='duration_bin',
                    values=computer.num_col
                )
                
                pivot_denom = heatmap_df.pivot(
                    index='eta_bin', 
                    columns='duration_bin',
                    values=computer.denom_col
                )

                pivot_tables[group] = pivot_table

                # Создаем тепловую карту
                fig = go.Figure()

                # Create hover text with numerator and denominator
                hover_text = []
                for i in range(len(pivot_table.index)):
                    hover_row = []
                    for j in range(len(pivot_table.columns)):
                        eta_val = pivot_table.index[i]
                        etr_val = pivot_table.columns[j]
                        ratio_val = pivot_table.values[i, j]
                        num_val = pivot_num.values[i, j] if not pd.isna(pivot_num.values[i, j]) else 0
                        denom_val = pivot_denom.values[i, j] if not pd.isna(pivot_denom.values[i, j]) else 0
                        hover_row.append(
                            f"ETA: {eta_val}<br>ETR: {etr_val}<br>Metric: {ratio_val:.4f}<br>Numerator: {int(num_val)}<br>Denominator: {int(denom_val)}"
                        )
                    hover_text.append(hover_row)

                heatmap = go.Heatmap(
                    z=pivot_table.values,
                    x=pivot_table.columns,
                    y=pivot_table.index,
                    colorscale='RdBu',
                    zmin=zmin1,
                    zmax=zmax1,
                    zmid=zmid1,
                    colorbar=dict(len=0.8),
                    text=hover_text,
                    hoverinfo='text'
                )

                fig.add_trace(heatmap)

                fig.update_layout(
                    title_text=f"{metric}: {group}",
                    width=750,
                    height=700,
                    template='plotly_white',
                    xaxis_title="ETR (seconds)",
                    yaxis_title="ETA (seconds)"
                )
                
                # Save the figure if PLOT_ROOT_PATH is provided
                if PLOT_ROOT_PATH:
                    # Save HTML version
                    filename = f"{PLOT_ROOT_PATH}/heatmaps/{EXP_ID}_{metric}_{group}.html"
                    fig.write_html(filename)
                    print(f"Saved HTML figure to {filename}")
                    
                    # Try to save PNG version, but don't crash if kaleido isn't properly configured
                    try:
                        png_filename = filename.replace('.html', '.png')
                        fig.write_image(png_filename)
                        print(f"Saved PNG figure to {png_filename}")
                    except ValueError as e:
                        if "kaleido package" in str(e):
                            print("Warning: Could not save PNG version. Install kaleido with:")
                            print("    pip install -U kaleido")
                            print("HTML version was still saved successfully.")
                        else:
                            raise e

                fig.show()
            
            pivot_table = (pivot_tables[groups[1]] - pivot_tables[groups[0]]) / pivot_tables[groups[0]]

            # Создаем тепловую карту
            fig = go.Figure()

            heatmap = go.Heatmap(
                z=pivot_table.values,
                x=pivot_table.columns,
                y=pivot_table.index,
                colorscale='RdBu',
                zmin=zmin2,
                zmax=zmax2,
                zmid=zmid2,
                colorbar=dict(len=0.8)
                )

            fig.add_trace(heatmap)

            fig.update_layout(
                title_text=f"{metric}: {groups[1]} minus {groups[0]} (relative)",
                width=750,
                height=700,
                template='plotly_white',
                xaxis_title="ETR (seconds)",
                yaxis_title="ETA (seconds)"
                )
                
            # Save the relative difference figure if PLOT_ROOT_PATH is provided
            if PLOT_ROOT_PATH:
                # Save HTML version
                filename = f"{PLOT_ROOT_PATH}/heatmaps/{EXP_ID}_{metric}_Rel_Diff.html"
                fig.write_html(filename)
                print(f"Saved HTML figure to {filename}")
                
                # Try to save PNG version, but don't crash if kaleido isn't properly configured
                try:
                    png_filename = filename.replace('.html', '.png')
                    fig.write_image(png_filename)
                    print(f"Saved PNG figure to {png_filename}")
                except ValueError as e:
                    if "kaleido package" in str(e):
                        print("Warning: Could not save PNG version. Install kaleido with:")
                        print("    pip install -U kaleido")
                        print("HTML version was still saved successfully.")
                    else:
                        raise e

            fig.show()
            return fig

# Usage example:
# plot_heatmap(df, numerator_func=custom_numerator_func, denominator_func=custom_denominator_func)
