# [MEP-2466] MAX BID ALGORITHM

## Общая информация
Этот репозиторий содержит исследования, пайплайны данных и экспериментальные артефакты, созданные в рамках проекта **«MAX BID»** (MEP-2466) в компании **inDriver**.  
Цель проекта — разработать, запустить и оценить новую логику бидов, которая уменьшает долю _плохих бидов_ водителей, сохраняя эффективность маркетплейса.

Кодовая база представляет собой коллекцию воспроизводимых Jupyter-ноутбуков, SQL/Redash-запросов и лёгких Python/Go-утилит, созданных в пилотной фазе.  
Это **не** монолитное приложение — каждая директория — автономное рабочее пространство под конкретный этап (анализ, выбор городов, симуляция, мониторинг и т. д.).

```
├── exp_anal/         # Анализ экспериментов (A/B + Switchback)
├── exp_cities/       # Анализ рынка и кластеризация для выбора городов
├── graphana_logs/    # Скрипты для работы с Grafana / Loki
├── min_step/         # Go- и Python-реализация новых алгоритмов бидов
├── monitor/          # Дашборды и алерты для запущенных экспериментов
├── other/            # Черновики и ad-hoc скрипты
└── simulation/       # Монте-Карло симуляции экспериментов
```

## Путеводитель по директориям

### 1. `exp_anal/`
• Ответвление внутреннего репозитория команды для Switchback-экспериментов.  
• **Расширен** аналитикой на уровне бидов: новый Redash-запрос (`queries/bids.sql`) выгружает сырые биды водителей из ClickHouse; метрики рассчитываются в `src/metrics.py`.  
• Включает **10 Jupyter-ноутбуков**, каждый из которых документирует анализ отдельного эксперимента MAX-BID (влияние на AR, GMV, share bad bids и т. д.).

### 2. `exp_cities/`
Инструменты для выбора оптимальных городов для запуска.

* Комплексный анализ рынка — комбинация спроса, предложения и метрик качества.
* В `src/` лежат SQL-шаблоны и вспомогательные функции (`buttons_shares_redash.sql`, `otherbid_share.py`, …).
* Ноутбуки показывают кластеризацию рынков и финальную процедуру отбора.

### 3. `graphana_logs/`
Небольшие утилиты (`check.py`, `graphana_check.ipynb`) для проверки потоков Grafana / Loki, на которые опирается мониторинг.

### 4. `min_step/`
Референсная реализация двух алгоритмов корректировки цен бидов на _минутном_ шаге:

* `bidmph_delta.py` — алгоритм «bid-MPH Δ».
* `bidmph_noexposure_delta.py` — «bid-MPH Δ (no exposure)».
* Версии на Go (`algorithm.go`, `custom_steps.go`) для интеграционных тестов / высокопроизводительных симуляций.

### 5. `monitor/`
Ноутбуки и скрипты для отслеживания живых экспериментов.  
Каждый город/эксперимент имеет отдельную папку в `monitor/cities`; графики генерируются автоматически через `monitor/src/calculations.py`.

### 6. `simulation/`
Среда Монте-Карло для стресс-тестирования логики бидов перед выкатом в прод.  
Ключевые точки входа:
* `draw_heatmap.py` — визуальная проверка покрытия.
* `get_agg_data.py`, `get_data.py` — загрузчики данных, которые используют ноутбуки.

### 7. `other/`
Разовые анализы и исторические скрипты, полезные в ходе исследования.

## Быстрый старт
1. Клонируйте репозиторий через **корпоративный** SSH-домен:
```bash
git clone git@indriver.github.com:gnusuev-indriver/badbids.git mep-2466-max-bid
cd mep-2466-max-bid
```
2. Создайте Python-окружение (>=3.8) и установите минимальные зависимости:
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt   # файл умышленно минимален
```
3. Основная аналитика находится в ноутбуках — открывайте их в JupyterLab или VS Code.

## Процесс вкладов
* Ветка `main` защищена: используйте feature-ветки и pull-requests.
* Большие дампы CSV/PQT и сырые ноутбуки **игнорируются** в git — для свежих данных используйте ClickHouse/Redash.
* Для исторических тяжёлых данных предпочитайте `dvc` или Git LFS.

## Лицензия
Внутренний проект inDriver — конфиденциально. 

## P.S.
Ридми написан с моих слов ллмкой. Могут быть домыслы и неточности. Ну вы знаете, как бывает.
