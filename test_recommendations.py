# test_recommendations.py
import pandas as pd
from services.recommendation_service import content_based_recommendations
from unittest.mock import patch

# Пример данных с нулевым прогрессом по теме 5 (theme_id = 5)
students = pd.DataFrame([{"id": 999, "name": "Тестовый Студент"}])
tasks = pd.DataFrame([
    {"id": 1, "section_id": 1, "description": "Задание по теме 5", "complexity": 2, "theme_id": 5, "theme_name": "Комбинаторика"},
    {"id": 2, "section_id": 1, "description": "Задание по Треугольникам", "complexity": 3, "theme_id": 2, "theme_name": "Треугольники"}
])
progress = pd.DataFrame([{"student_id": 999, "theme_id": 5, "progress": 0.0}])
completed_ids = []

@patch("repository.student_repo.get_student_by_id", return_value=students)
@patch("repository.task_repo.get_completed_task_ids", return_value=completed_ids)
@patch("repository.task_repo.get_tasks_with_themes", return_value=tasks)
@patch("repository.progress_repo.get_student_theme_progress", return_value=progress)
@patch("repository.forms_repo.get_student_preferences", return_value=[])
def test_zero_progress_recommendation(mock1, mock2, mock3, mock4, mock5):
    result = content_based_recommendations(999)
    assert not result.empty, "Ожидались рекомендации для студента с нулевым прогрессом"
    assert 5 in result['theme_id'].values, "Рекомендуемая тема должна быть та, где прогресс = 0"
    print("✅ Тест 1 пройден: студенту с нулевым прогрессом предлагается нужная тема")

@patch("repository.student_repo.get_student_by_id", return_value=students)
@patch("repository.task_repo.get_completed_task_ids", return_value=completed_ids)
@patch("repository.task_repo.get_tasks_with_themes", return_value=tasks)
@patch("repository.progress_repo.get_student_theme_progress", return_value=progress)
@patch("repository.forms_repo.get_student_preferences", return_value=["Треугольники"])
def test_preference_recommendation(mock1, mock2, mock3, mock4, mock5):
    result = content_based_recommendations(999)
    assert not result.empty, "Ожидались рекомендации по предпочтениям"
    assert "Треугольники" in result['theme_name'].values, "Тема из предпочтений должна быть рекомендована"
    print("✅ Тест 2 пройден: предпочтения из анкеты учитываются корректно")

if __name__ == "__main__":
    test_zero_progress_recommendation()
    test_preference_recommendation()