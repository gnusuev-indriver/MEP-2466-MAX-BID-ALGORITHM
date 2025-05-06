import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from scipy.stats import gaussian_kde
import plotly.express as px  # Необходимо для доступа к палитрам цветов

###
### Анимированное распределение
###
def plot_density_anime(df, metrics, group_col='group_name',
                                 groups=['Before', 'Control', 'A'],
                                 line_styles=['dot', 'dash', 'solid'],
                                 height=600, width=1200):
    """
    Строит распределения нескольких метрик на одном графике с возможностью анимации по параметру bandwidth.

    Параметры:
    df (DataFrame): DataFrame с данными
    metrics (list): Список метрик для построения
    group_col (str): Название столбца с группами
    groups (list): Список групп для сравнения
    line_styles (list): Список стилей линий для разных групп
    height (int): Высота графика
    width (int): Ширина графика
    """
    fig = go.Figure()

    # Добавляем необходимые цвета
    palette = px.colors.qualitative.Alphabet + px.colors.qualitative.Dark24 + px.colors.qualitative.Light24
    colors = palette[:len(metrics)]
    
    # Если количество метрик превышает размер палитры, генерируем новые цвета
    if len(metrics) > len(colors):
        def generate_color(index):
            hue = int(360 * index / len(metrics))
            return f'hsl({hue}, 70%, 50%)'
        additional_colors = [generate_color(i) for i in range(len(colors), len(metrics))]
        colors += additional_colors

    # Определяем список значений параметра bandwidth для анимации
    bandwidths = [round(x * 0.05, 2) for x in range(1, 13)]  

    trace_order = []  # Для отслеживания порядка трасс (метрика, группа)

    # Инициализируем трассы с начальным bandwidth
    initial_bw = bandwidths[0]
    for metric_idx, (metric, color) in enumerate(zip(metrics, colors)):
        for group_idx, group in enumerate(groups):
            # Получаем данные группы как numpy array
            data = df[df[group_col] == group][metric].dropna().values.astype(np.float64)
            
            try:
                if len(data) > 0:
                    # Вычисляем KDE
                    kde = gaussian_kde(data, bw_method=initial_bw)
                    kde_x = np.linspace(data.min(), data.max(), 200)
                    kde_y = kde(kde_x)
                    
                    # Добавляем трассу
                    trace = go.Scatter(
                        x=kde_x,
                        y=kde_y,
                        name=f'{metric} ({group})',
                        line=dict(
                            color=color,
                            dash=line_styles[group_idx % len(line_styles)],
                        ),
                        opacity=0.7,
                        visible=True  # Трассы видимы по умолчанию
                    )
                    fig.add_trace(trace)
                    trace_order.append((metric, group))
                else:
                    # Добавляем пустую трассу, чтобы сохранить порядок
                    trace = go.Scatter(
                        x=[],
                        y=[],
                        name=f'{metric} ({group})',
                        line=dict(
                            color=color,
                            dash=line_styles[group_idx % len(line_styles)],
                        ),
                        opacity=0.7,
                        visible=True
                    )
                    fig.add_trace(trace)
                    trace_order.append((metric, group))
            except Exception as e:
                print(f"Error processing {metric} for {group}: {e}")
    
    # Создаём кадры для каждого значения bandwidth
    frames = []
    for bw in bandwidths:
        frame_traces = []
        for metric, group in trace_order:
            # Получаем данные группы как numpy array
            data = df[df[group_col] == group][metric].dropna().values.astype(np.float64)
            if len(data) > 0:
                # Вычисляем KDE с текущим bandwidth
                kde = gaussian_kde(data, bw_method=bw)
                kde_x = np.linspace(data.min(), data.max(), 200)
                kde_y = kde(kde_x)
                
                frame_traces.append({'x': kde_x, 'y': kde_y})
            else:
                # Если нет данных, оставляем пустыми
                frame_traces.append({'x': [], 'y': []})
        frames.append(go.Frame(data=[{'x': ft['x'], 'y': ft['y']} for ft in frame_traces],
                               name=str(bw)))
    
    fig.frames = frames

    # Создаём слайдер для выбора параметра bandwidth
    sliders = [{
        "active": 0,
        "yanchor": "top",
        "xanchor": "left",
        "currentvalue": {
            # "font": {"size": 20},
            "prefix": "Bandwidth: ",
            "visible": True,
            "xanchor": "right"
        },
        "transition": {"duration": 300, "easing": "cubic-in-out"},
        "pad": {"b": 10, "t": 50},
        "steps": [
            {
                "args": [
                    [frame.name],
                    {"frame": {"duration": 300, "redraw": True},
                     "mode": "immediate",
                     "fromcurrent": True}
                ],
                "label": str(frame.name),
                "method": "animate"
            } for frame in fig.frames
        ]
    }]

    # Настраиваем внешний вид
    fig.update_layout(
        height=height,
        width=width,
        title='Metrics Distribution',
        # title_x=0.5,
        xaxis_title="Value",
        yaxis_title="Density",
        template="plotly_white",
        showlegend=True,
        sliders=sliders  # Добавляем слайдер
    )

    return fig

# # Пример использования
# fig = plot_multiple_metrics_plotly(df_metrics_grouped, 
#                                    metrics=df_metrics_grouped.columns[3:65])
# fig.show()



###
### Один фрейм анимации распределение
###
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from scipy.stats import gaussian_kde

def plot_density(df, metrics, group_col='group_name',
                               groups=['Before', 'Control', 'A'],
                               line_styles=['dot', 'dash', 'solid'],
                               height=600, width=800):
    """
    Строит распределения нескольких метрик на одном графике
    
    Параметры:
    df (DataFrame): DataFrame с данными
    metrics (list): Список метрик для построения
    group_col (str): Название столбца с группами
    groups (list): Список групп для сравнения
    colors (list): Список цветов для групп
    line_styles (list): Список стилей линий для разных метрик
    height (int): Высота графика
    width (int): Ширина графика
    """
    fig = go.Figure()

    # Добавляем необходимые цвета
    colors = []
    palette = px.colors.qualitative.Alphabet + px.colors.qualitative.Dark24 + px.colors.qualitative.Light24
    colors += palette[:len(metrics) - len(colors)]
    
    # Если все предопределенные палитры исчерпаны, генерируем новые цвета
    if len(colors) < len(metrics):
        def generate_color(index):
            hue = int(360 * index / len(metrics))
            return f'hsl({hue}, 70%, 50%)'
        colors += [generate_color(i) for i in range(len(colors), len(metrics))]

    # Для каждой метрики
    for metric_idx, (metric, color) in enumerate(zip(metrics, colors)):
        print(metric_idx, metric)
        # Для каждой группы
        for group_idx, group in enumerate(groups):
            # Получаем данные группы как numpy array
            data = df[df[group_col] == group][metric]
            data = np.array(data.values, dtype=np.float64)
            data = data[~np.isnan(data)]  # Удаляем NaN значения
            
            if len(data) > 0:
                # Вычисляем KDE
                kde_x = np.linspace(np.min(data), np.max(data), 200)
                data_reshaped = data.reshape(-1, 1)
                kde = gaussian_kde(data_reshaped.T,0.1)
                kde_y = kde(kde_x)
                
                # Добавляем график
                fig.add_trace(
                    go.Scatter(
                        x=kde_x,
                        y=kde_y,
                        name=f'{group} - {metric}',
                        line=dict(
                            color=color,
                            dash=line_styles[group_idx % len(line_styles)],
                        ),
                        opacity=0.7
                    )
                )
    
    # Настраиваем внешний вид
    fig.update_layout(
        height=height,
        width=width,
        title='Сравнение распределений метрик',
        title_x=0.5,
        xaxis_title="Value",
        yaxis_title="Density",
        template="plotly_white",
        showlegend=True
    )
    
    return fig

# fig = plot_multiple_metrics_plotly(df_metrics_grouped, 
#                                  metrics=df_metrics_grouped.columns[3:65])
# fig.show()



###
### Простое распределение
###
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def plot_density_simple(data, metric, bins=30, height=600, width=1200):
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.3,
        subplot_titles=("Control Group", "Group A")
    )

    # Remove extreme values (above 99th percentile)
    data = data[data[metric] <= data[metric].quantile(0.99)]

    # Define common range
    x_range = [data[metric].min(), data[metric].max()]

    # Plot Control group
    fig.add_trace(go.Histogram(
        x=data[data['group_name'] == 'Control'][metric],
        nbinsx=bins,
        histnorm='probability density',
        marker_color='steelblue',
        opacity=0.75,
        name='Control'
    ), row=1, col=1)

    # Plot Group A
    fig.add_trace(go.Histogram(
        x=data[data['group_name'] == 'A'][metric],
        nbinsx=bins,
        histnorm='probability density',
        marker_color='indianred',
        opacity=0.75,
        name='Group A'
    ), row=2, col=1)

    # Update layout
    fig.update_layout(
        height=height,
        width=width,
        title_text=f"Metric: {metric}",
        showlegend=False,
        bargap=0.05,
        template="simple_white"
    )

    # Apply x-range to both subplots
    fig.update_xaxes(range=x_range, title_text='Value', row=2, col=1)
    fig.update_yaxes(title_text="Density", row=1, col=1)
    fig.update_yaxes(title_text="Density", row=2, col=1)

    return fig


# plot_density_histogram_simple(df_metrics_grouped, 'orders_count')


import plotly.graph_objects as go

def plot_density_overlay(data, metric, bins=30, height=600, width=1200):
    # Remove extreme values (above 99th percentile)
    data = data[data[metric] <= data[metric].quantile(0.99)]

    # Define common range
    x_range = [data[metric].min(), data[metric].max()]

    # Create figure
    fig = go.Figure()

    # Control group
    fig.add_trace(go.Histogram(
        x=data[data['group_name'] == 'Control'][metric],
        nbinsx=bins,
        histnorm='probability density',
        marker_color='steelblue',
        opacity=0.5,
        name='Control'
    ))

    # Group A
    fig.add_trace(go.Histogram(
        x=data[data['group_name'] == 'A'][metric],
        nbinsx=bins,
        histnorm='probability density',
        marker_color='indianred',
        opacity=0.5,
        name='Group A'
    ))

    # Update layout
    fig.update_layout(
        title_text=f"Metric: {metric}",
        xaxis_title='Value',
        yaxis_title='Density',
        bargap=0.05,
        barmode='overlay',  # Enables overlapping histograms
        template="simple_white",
        height=height,
        width=width,
        xaxis_range=[data[metric].min(), data[metric].max()]
    )

    return fig
