# services/analyze_service.py
import pandas as pd
from repository import progress_repo, task_repo


def analyze_student_readiness(student_id):
    """
    Анализ готовности студента по каждой теме с текстовой оценкой и рекомендацией.
    """
    progress = progress_repo.get_student_theme_progress(student_id)
    themes = task_repo.get_themes()

    merged = progress.merge(themes, on="theme_id", how="left")
    merged = merged[["theme_id", "theme_name", "progress"]]

    def label(row):
        p = row["progress"]
        if p >= 70:
            return "готов"
        elif p >= 40:
            return "частично готов"
        else:
            return "не готов"

    def recommendation(row):
        state = row["состояние"]
        if state == "не готов":
            return "изучить тему с нуля"
        elif state == "частично готов":
            return "закрепить материал и решить задачи"
        else:
            return "можно переходить дальше"

    merged["состояние"] = merged.apply(label, axis=1)
    merged["рекомендация"] = merged.apply(recommendation, axis=1)

    return merged.sort_values(by="progress")