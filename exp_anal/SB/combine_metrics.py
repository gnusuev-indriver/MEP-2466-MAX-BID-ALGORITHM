#!/usr/bin/env python3
"""
Скрипт для объединения файлов metrics_total_tbl.csv из всех экспериментов
в один общий CSV файл с добавлением колонки exp_id
"""

import pandas as pd
import os
import glob
import argparse
from pathlib import Path

def combine_metrics_files(data_dir, output_file=None):
    """
    Объединяет все файлы metrics_total_tbl.csv из папок экспериментов в один CSV
    
    Args:
        data_dir (str): Путь к папке с экспериментами (где находятся папки exp_id=XXXX)
        output_file (str): Путь к выходному файлу. По умолчанию combined_metrics.csv
    """
    
    # Поиск всех файлов metrics_total_tbl.csv в папках экспериментов
    pattern = os.path.join(data_dir, "exp_id=*", "metrics_total_tbl.csv")
    csv_files = glob.glob(pattern)
    
    if not csv_files:
        print(f"Не найдено файлов metrics_total_tbl.csv в папке {data_dir}")
        return
    
    print(f"Найдено {len(csv_files)} файлов для объединения:")
    
    combined_data = []
    
    for file_path in sorted(csv_files):
        # Извлекаем exp_id из пути файла
        folder_name = os.path.basename(os.path.dirname(file_path))
        exp_id = folder_name.replace("exp_id=", "")
        
        try:
            # Читаем CSV файл
            df = pd.read_csv(file_path)
            # Добавляем колонку exp_id
            df['exp_id'] = exp_id
            combined_data.append(df)
            print(f"  - {file_path} (exp_id={exp_id}): {len(df)} строк")
            
        except Exception as e:
            print(f"Ошибка при чтении файла {file_path}: {e}")
    
    if not combined_data:
        print("Нет данных для объединения")
        return
    
    # Объединяем все данные
    result_df = pd.concat(combined_data, ignore_index=True)
    
    # Переставляем колонки, чтобы exp_id была первой
    cols = ['exp_id'] + [col for col in result_df.columns if col != 'exp_id']
    result_df = result_df[cols]
    
    # Определяем имя выходного файла
    if output_file is None:
        # Сохраняем в папке SB (на уровень выше от data)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_file = os.path.join(script_dir, "combined_metrics.csv")
    
    # Сохраняем результат
    result_df.to_csv(output_file, index=False)
    
    print(f"\nРезультат сохранен в {output_file}")
    print(f"Общее количество строк: {len(result_df)}")
    print(f"Количество экспериментов: {result_df['exp_id'].nunique()}")
    print(f"Эксперименты: {sorted(result_df['exp_id'].unique())}")

def main():
    # Определяем путь к папке data относительно расположения скрипта
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_data_dir = os.path.join(script_dir, "data")
    
    parser = argparse.ArgumentParser(
        description="Объединяет файлы metrics_total_tbl.csv из всех экспериментов"
    )
    parser.add_argument(
        "--data-dir", 
        default=default_data_dir,
        help="Путь к папке с экспериментами (по умолчанию: ./data)"
    )
    parser.add_argument(
        "--output", 
        help="Путь к выходному файлу (по умолчанию: combined_metrics.csv в папке SB)"
    )
    
    args = parser.parse_args()
    
    if not os.path.exists(args.data_dir):
        print(f"Папка {args.data_dir} не существует")
        return
    
    combine_metrics_files(args.data_dir, args.output)

if __name__ == "__main__":
    main()