# main.py
import logging
import argparse
import os
import sqlite3
import pandas as pd

from services.recommendation_service import content_based_recommendations
from services.collaborative_service import build_model, get_recommendations
from services.analyze_service import analyze_student_readiness
from markdown_report import save_markdown_report, save_readiness_report
from repository import task_repo
from db import load_data
from validator import run_all

# Настройка логирования
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/system.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)

# Парсинг аргументов командной строки
parser = argparse.ArgumentParser(description="Recommendation System")
parser.add_argument('--student-id', type=int, help='ID студента')
parser.add_argument('--mode', type=str, choices=['content', 'collab', 'hybrid'], help='Тип рекомендаций')
parser.add_argument('--export-path', type=str, help='Путь для сохранения рекомендаций в CSV')
args = parser.parse_args()

# Запросы, если аргументы не переданы
student_id = args.student_id if args.student_id else int(input("Введите ID студента: "))
mode = args.mode
if not mode:
    print("Выберите тип рекомендаций:")
    print("1. Content-Based")
    print("2. Collaborative Filtering")
    print("3. Гибридные рекомендации")
    choice = input("Ваш выбор (1/2/3): ")
    mode = {'1': 'content', '2': 'collab', '3': 'hybrid'}.get(choice)

logging.info(f"Запуск системы для student_id={student_id}, режим={mode}")

# Загружаем данные из БД
students, tasks, lessons, lesson_tasks, exam_results, exam_tasks, student_theme_progress = load_data()
with sqlite3.connect("your_database.db") as conn:
    forms = pd.read_sql_query("SELECT * FROM Forms", conn)

# Валидация данных
run_all(tasks, forms, students, student_theme_progress)

recommendations = pd.DataFrame()

if mode == "content":
    recommendations = content_based_recommendations(student_id)
    logging.info(f"Content-Based: получено рекомендаций: {len(recommendations)}")
    print("\nContent-Based рекомендации:")
    print(recommendations if not recommendations.empty else "Нет рекомендаций.")

elif mode == "collab":
    model, matrix = build_model(exam_results)
    if model:
        recommendations = get_recommendations(student_id, model, matrix, exam_tasks, tasks)
        logging.info(f"Collaborative Filtering: получено рекомендаций: {len(recommendations)}")
        print("\nCollaborative Filtering рекомендации:")
        print(recommendations if not recommendations.empty else "Нет рекомендаций.")
    else:
        logging.error("Не удалось построить модель совместной фильтрации")
        print("Не удалось построить модель.")

elif mode == "hybrid":
    model, matrix = build_model(exam_results)
    cb_recs = content_based_recommendations(student_id)
    cf_recs = get_recommendations(student_id, model, matrix, exam_tasks, tasks) if model else pd.DataFrame()

    hybrid = pd.concat([cb_recs, cf_recs]).drop_duplicates(subset='id', keep='first')
    themes = task_repo.get_themes()
    if 'theme_name' not in hybrid.columns:
        hybrid = hybrid.merge(themes, on='theme_id', how='left')
    else:
        hybrid = hybrid.drop(columns='theme_name').merge(themes, on='theme_id', how='left')

    recommendations = hybrid
    logging.info(f"Гибридных рекомендаций получено: {len(recommendations)}")
    print("\nГибридные рекомендации:")
    print(recommendations if not recommendations.empty else "Нет рекомендаций.")

else:
    logging.warning(f"Неверный ввод режима: {mode}")
    print("Некорректный выбор.")

# Сохраняем результат, если указан путь
if args.export_path and not recommendations.empty:
    try:
        recommendations.to_csv(args.export_path, index=False)
        logging.info(f"Рекомендации сохранены в {args.export_path}")
        print(f"\nРекомендации сохранены в файл: {args.export_path}")

        # Markdown-отчет
        md_path = args.export_path.replace('.csv', '.md')
        save_markdown_report(recommendations, md_path, student_id)

        # Анализ готовности
        readiness_df = analyze_student_readiness(student_id)
        readiness_path = args.export_path.replace('.csv', '_readiness.csv')
        readiness_df.to_csv(readiness_path, index=False)
        print(f"🧠 Анализ готовности сохранен в: {readiness_path}")

        # Markdown по готовности
        readiness_md = readiness_path.replace('.csv', '.md')
        save_readiness_report(readiness_df, readiness_md, student_id)

    except Exception as e:
        logging.error(f"Ошибка при сохранении CSV: {e}")
        print(f"Ошибка при сохранении файла: {e}")