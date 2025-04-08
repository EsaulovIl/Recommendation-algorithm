# services/collaborative_service.py
import pandas as pd
from sklearn.neighbors import NearestNeighbors
from repository import task_repo

def build_model(exam_results):
    """Создает модель KNN на основе результатов экзаменов."""
    try:
        matrix = pd.pivot_table(exam_results, index='student_id', columns='exam_id', values='grade').fillna(0)
        model = NearestNeighbors(metric='cosine', algorithm='brute')
        model.fit(matrix.values)
        return model, matrix
    except Exception as e:
        print(f"Ошибка при построении модели: {e}")
        return None, None

def get_recommendations(student_id, model, matrix, exam_tasks, tasks):
    try:
        index = matrix.index.get_loc(student_id)
        distances, indices = model.kneighbors([matrix.iloc[index]], n_neighbors=3)
        neighbors = matrix.iloc[indices.flatten()[1:]]

        task_ids = []
        for exam_id in neighbors.columns:
            task_ids.extend(exam_tasks[exam_tasks['exam_id'] == exam_id]['task_id'].tolist())

        completed = task_repo.get_completed_task_ids(student_id)
        task_ids = list(set(task_ids) - set(completed))

        recommended_tasks = tasks[tasks['id'].isin(task_ids)]

        themes = task_repo.get_themes()
        recommended_tasks = recommended_tasks.merge(themes, on='theme_id', how='left')

        recommended_tasks['explanation'] = "решали похожие ученики"
        recommended_tasks['source'] = "collaborative"

        return recommended_tasks[['id', 'section_id', 'description', 'complexity', 'theme_id', 'theme_name', 'explanation', 'source']]
    except Exception as e:
        print(f"Ошибка в рекомендациях: {e}")
        return pd.DataFrame()
