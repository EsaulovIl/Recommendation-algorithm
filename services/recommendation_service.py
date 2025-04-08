# services/recommendation_service.py
from repository import student_repo, task_repo, forms_repo, progress_repo
import pandas as pd

def content_based_recommendations(student_id):
    """
    Гибридные рекомендации на основе содержания:
    – предпочтения из анкеты (Forms)
    – темы с низким прогрессом
    – исключение уже выполненных заданий
    """
    student_info = student_repo.get_student_by_id(student_id)
    if student_info.empty:
        print("Нет информации о студенте.")
        return pd.DataFrame()

    preferences = forms_repo.get_student_preferences(student_id)
    completed_task_ids = task_repo.get_completed_task_ids(student_id)
    tasks_with_theme = task_repo.get_tasks_with_themes()
    student_progress = progress_repo.get_student_theme_progress(student_id)

    progress_threshold = 70.0
    low_progress_themes = student_progress[student_progress['progress'] < progress_threshold]['theme_id'].tolist()

    candidate_tasks = tasks_with_theme[~tasks_with_theme['id'].isin(completed_task_ids)]

    explanations = []
    filtered_tasks = []

    for _, task in candidate_tasks.iterrows():
        reason = []
        if preferences and any(pref.lower() in (task['theme_name'] or '').lower() for pref in preferences):
            reason.append("интерес ученика")
        if task['theme_id'] in low_progress_themes:
            reason.append("низкий прогресс по теме")
        if not reason:
            reason.append("не выполнено ранее")

        task_with_reason = task.copy()
        task_with_reason['explanation'] = ", ".join(reason)
        task_with_reason['source'] = "content"
        filtered_tasks.append(task_with_reason)

    recommended_tasks = pd.DataFrame(filtered_tasks)
    return recommended_tasks[['id', 'section_id', 'description', 'complexity', 'theme_id', 'theme_name', 'explanation', 'source']]
