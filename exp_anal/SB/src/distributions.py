import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from scipy.stats import gaussian_kde
import plotly.express as px  
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from scipy.stats import gaussian_kde
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import os

###
### Анимированное распределение
###
def plot_density_anime(df, metrics, group_col='group_name',
                                 groups=['Before', 'Control', 'A'],
                                 line_styles=['dot', 'dash', 'solid'],
                                 height=600, width=1200,
                                 title='Metrics Distribution'):
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
        title=title,
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


###
### Простое распределение
###
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

###
### Перекрывающиеся гистограммы
###
def plot_density_overlay(data, metric, bins=30, height=600, width=1100, title=None, PLOT_ROOT_PATH=None, EXP_ID=None):
    # Slice only needed data
    subset = data.loc[data['group_name'].isin(['Control', 'A']), [metric, 'group_name']].copy()

    # Remove extreme values based on group-specific percentiles
    lower = min(subset[subset['group_name'] == 'Control'][metric].quantile(0.01), 
                subset[subset['group_name'] == 'A'][metric].quantile(0.01))
    upper = max(subset[subset['group_name'] == 'Control'][metric].quantile(0.99), 
                subset[subset['group_name'] == 'A'][metric].quantile(0.99))
    
    subset[metric] = subset[metric].clip(lower=lower, upper=upper)

    # Create figure
    fig = go.Figure()

    # Calculate bin edges and bin size
    bin_edges = np.linspace(lower, upper, bins + 1)
    bin_size = bin_edges[1] - bin_edges[0]

    # Add histogram for Control group
    fig.add_trace(go.Histogram(
        x=subset[subset['group_name'] == 'Control'][metric],
        xbins=dict(start=lower, end=upper, size=bin_size),
        histnorm='probability',
        marker_color='steelblue',
        opacity=0.5,
        name='Control'
    ))

    # Add histogram for Group A
    fig.add_trace(go.Histogram(
        x=subset[subset['group_name'] == 'A'][metric],
        xbins=dict(start=lower, end=upper, size=bin_size),
        histnorm='probability',
        marker_color='indianred',
        opacity=0.5,
        name='Group A'
    ))

    # Cleanup
    del subset

    # Layout
    fig.update_layout(
        title_text=f"Metric: {metric}, {title}",
        xaxis_title='Value',
        yaxis_title='Probability',
        bargap=0.05,
        barmode='overlay',
        template="simple_white",
        height=height,
        width=width,
    )
    
    # Save the figure if PLOT_ROOT_PATH is provided
    if PLOT_ROOT_PATH:
        # Create the output directory if it doesn't exist
        os.makedirs(f"{PLOT_ROOT_PATH}/distributions", exist_ok=True)
        
        # Save the figure
        if title:
            filename = f"{PLOT_ROOT_PATH}/distributions/{EXP_ID}_{metric}_{title.replace(' ', '_')}.html"
        else:
            filename = f"{PLOT_ROOT_PATH}/distributions/{EXP_ID}_{metric}.html"
        
        # Save HTML version
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

    return fig
