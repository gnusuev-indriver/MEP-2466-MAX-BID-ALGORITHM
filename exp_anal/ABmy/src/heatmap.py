import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from typing import Callable, List, Optional, Tuple


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
    ):
        self.df = df
        self.group_cols = group_cols
        self.numerator_func = numerator_func
        self.denominator_func = denominator_func
        # self.num_col = num_col
        # self.denom_col = denom_col
        self.ratio_col = ratio_col
        self.min_samples = min_samples

    def compute(self) -> pd.DataFrame:
        # Вычисляем знаменатель
        denom_df = self.denominator_func(self.df, self.group_cols)
        denom_col = list(set(denom_df.columns) - set(self.group_cols))[0]
        # denom_df = denom_df.rename(columns={denom_value_col: self.denom_col})
        
        # Вычисляем числитель
        numer_df = self.numerator_func(self.df, self.group_cols)
        num_col = list(set(numer_df.columns) - set(self.group_cols))[0]
        # numer_df = numer_df.rename(columns={numer_value_col: self.num_col})

        # Объединяем
        merged = pd.merge(denom_df, numer_df, on=self.group_cols, how="left")
        merged[num_col] = merged[num_col].fillna(0)
        merged[self.ratio_col] = merged[num_col] / merged[denom_col]

        # Фильтрация
        # filtered = merged[merged[denom_col] >= self.min_samples]
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

def plot_heatmap(df, metric, numerator_func=calc_algo_mph, denominator_func=calc_total, min_samples=10, groups=['Control', 'A']):
    # Проверяем наличие необходимых колонок
    required_cols = ['group_name', 'eta', 'duration_in_min']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Колонка {col} отсутствует в данных")
        
    # Определяем максимальные значения для бинов
    df['duration_sec'] = df['duration_in_min'] * 60
    eta_percentile = df['eta'].quantile(0.99)
    eta_max = int(np.ceil(eta_percentile / 60.0)) * 60
    duration_percentile = df['duration_sec'].quantile(0.99)
    duration_max = int(np.ceil(duration_percentile / 60.0)) * 60 
    # max_eta = int(np.ceil(df['eta'].max() / 60.0)) * 60
    # max_duration = int(np.ceil(df['duration_sec'].max() / 60.0)) * 60

    # Создаем бины для eta и duration_in_min
    df['eta_bin'] = pd.cut(
         df['eta'],
         bins=np.arange(0, eta_max, 60),
         labels=[f"{i}" for i in range(0, eta_max - 60, 60)],
         right=False
         )

    df['duration_bin'] = pd.cut(
        df['duration_sec'],
        bins=np.arange(0, duration_max, 60*5),
        labels=[f"{i}" for i in range(0, duration_max - 60*5, 60*5)],
        right=False
        )

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

                # Используем класс RatioBinnedComputer
                computer = RatioBinnedComputer(
                    df=df_geo_type_group,
                    group_cols=['duration_bin', 'eta_bin'],
                    numerator_func=numerator_func,
                    denominator_func=denominator_func,
                    # num_col='algo_count',
                    # denom_col='count',
                    ratio_col=metric,
                    min_samples=min_samples
                )

                heatmap_df = computer.compute()

                if heatmap_df.empty:
                    print(f"Нет бинов с количеством записей >= {min_samples} для {geo}, {type_name}, {group}")
                    continue

                pivot_table = computer.to_pivot(
                    index='duration_bin', 
                    columns='eta_bin', 
                    values=computer.ratio_col
                )

                pivot_tables[group] = pivot_table

                # Создаем тепловую карту
                fig = go.Figure()

                heatmap = go.Heatmap(
                    z=pivot_table.values,
                    x=pivot_table.columns,
                    y=pivot_table.index,
                    colorscale='RdBu',
                    zmid=0.0,
                    colorbar=dict(title="Доля algo_bidmph", len=0.8)
                )

                fig.add_trace(heatmap)

                fig.update_layout(
                    title_text=f"{metric}: {group}",
                    width=900,
                    height=700,
                    template='plotly_white',
                    xaxis_title="ETA (seconds)",
                    yaxis_title="Duration (seconds)"
                )

                fig.show()
            
            pivot_table = pivot_tables[groups[1]] - pivot_tables[groups[0]]

            # Создаем тепловую карту
            fig = go.Figure()

            heatmap = go.Heatmap(
                z=pivot_table.values,
                x=pivot_table.columns,
                y=pivot_table.index,
                colorscale='RdBu',
                zmid=0.0,
                colorbar=dict(title="Доля algo_bidmph", len=0.8)
                )

            fig.add_trace(heatmap)

            fig.update_layout(
                title_text=f"{metric}: {groups[1]} minus {groups[0]}",
                width=900,
                height=700,
                template='plotly_white',
                xaxis_title="ETA (seconds)",
                yaxis_title="Duration (seconds)"
                )

            fig.show()

# Example of custom numerator function with arbitrary column name
def custom_numerator_func(df, group_cols):
    result_df = (
        df[df['algo_name_new'] == 'algo_bidmph']
        .groupby(group_cols)
        .size()
        .reset_index(name="my_custom_column_name")
    )
    return result_df, "my_custom_column_name"

# Example of custom denominator function with arbitrary column name
def custom_denominator_func(df, group_cols):
    result_df = (
        df.groupby(group_cols)
        .size()
        .reset_index(name="another_custom_name")
    )
    return result_df, "another_custom_name"

# Usage example:
# plot_heatmap(df, numerator_func=custom_numerator_func, denominator_func=custom_denominator_func)
