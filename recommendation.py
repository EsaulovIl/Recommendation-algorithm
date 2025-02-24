import sqlite3
import pandas as pd
from sklearn.neighbors import NearestNeighbors
import db  # модуль с функциями создания и загрузки данных

def content_based_recommendations(student_id, students, tasks, lessons, lesson_tasks, student_theme_progress):
    """
    Гибридные рекомендации на основе содержания:
    – учитываются предпочтения студента (из Forms)
    – учитывается прогресс по темам (StudentThemeProgress): если процент освоения ниже порогового,
      задания по данной теме рекомендованы
    – исключаются уже выполненные задания
    """
    try:
        # Проверяем, что студент существует
        student_info = students[students['id'] == student_id]
        if student_info.empty:
            print("Нет информации о студенте.")
            return pd.DataFrame()

        # Получаем предпочтения студента из Forms
        try:
            conn = sqlite3.connect('your_database.db')
            preferences_query = f"SELECT preferences FROM Forms WHERE student_id = {student_id}"
            student_preferences_df = pd.read_sql_query(preferences_query, conn)
            conn.close()
            if not student_preferences_df.empty:
                preferences = student_preferences_df['preferences'].iloc[0].split(', ')
            else:
                preferences = []
        except sqlite3.Error as e:
            print(f"Ошибка при получении предпочтений: {e}")
            preferences = []

        # Получаем список выполненных заданий для студента
        try:
            conn = sqlite3.connect('your_database.db')
            completed_tasks_df = pd.read_sql_query(f"""
                SELECT DISTINCT T.id
                FROM Tasks T
                JOIN Lesson_tasks LT ON T.id = LT.task_id
                JOIN Lessons L ON LT.lesson_id = L.id
                JOIN Schedule_lessons SL ON L.id = SL.lesson_id
                JOIN Schedule S ON SL.schedule_id = S.id
                WHERE S.student_id = {student_id}
            """, conn)
            conn.close()
            completed_task_ids = completed_tasks_df['id'].tolist()
        except sqlite3.Error as e:
            print(f"Ошибка при получении выполненных заданий: {e}")
            completed_task_ids = []

        # Загружаем задания с информацией о теме (объединяем с Themes)
        conn = sqlite3.connect('your_database.db')
        tasks_with_theme = pd.read_sql_query('''
            SELECT T.id, T.section_id, T.description, T.complexity, T.theme_id, th.name AS theme_name
            FROM Tasks T
            JOIN Themes th ON T.theme_id = th.id
        ''', conn)
        conn.close()

        # Исключаем выполненные задания
        candidate_tasks = tasks_with_theme[~tasks_with_theme['id'].isin(completed_task_ids)]

        # Определяем темы, в которых у студента низкий прогресс
        progress_threshold = 70.0
        student_prog = student_theme_progress[student_theme_progress['student_id'] == student_id]
        low_progress_themes = student_prog[student_prog['progress'] < progress_threshold]['theme_id'].tolist()

        # Если у студента есть предпочтения, фильтруем кандидатов по совпадению с названием темы
        if preferences:
            candidate_tasks = candidate_tasks[
                candidate_tasks['theme_name'].str.contains('|'.join(preferences), case=False, na=False)
            ]

        # Рекомендуем задания из тем с низким прогрессом
        recommended_tasks = candidate_tasks[candidate_tasks['theme_id'].isin(low_progress_themes)]

        # Если по темам с низким прогрессом нет кандидатов, возвращаем все не выполненные задания
        if recommended_tasks.empty:
            recommended_tasks = candidate_tasks

        return recommended_tasks[['id', 'section_id', 'description', 'complexity', 'theme_id']]

    except Exception as e:
        print(f"Ошибка в content_based_recommendations: {e}")
        return pd.DataFrame()


def collaborative_filtering(exam_results):
    """Создает модель совместной фильтрации на основе оценок экзаменов."""
    try:
        exam_scores_matrix = pd.pivot_table(exam_results, index='student_id', columns='exam_id',
                                            values='grade').fillna(0)
        model_knn = NearestNeighbors(metric='cosine', algorithm='brute')
        model_knn.fit(exam_scores_matrix.values)
        return model_knn, exam_scores_matrix
    except Exception as e:
        print(f"Ошибка в collaborative_filtering: {e}")
        return None, None


def get_collaborative_recommendations(student_id, model_knn, exam_scores_matrix, tasks, exam_tasks, n_neighbors=2):
    """Возвращает рекомендации с помощью совместной фильтрации."""
    try:
        student_index = exam_scores_matrix.index.get_loc(student_id)
        n_neighbors = min(n_neighbors, len(exam_scores_matrix) - 1)
        distances, indices = model_knn.kneighbors([exam_scores_matrix.iloc[student_index]], n_neighbors=n_neighbors + 1)
        similar_student_indices = indices.flatten()[1:]
        similar_students_exams = exam_scores_matrix.iloc[similar_student_indices]

        recommended_task_ids = []
        for exam_id in similar_students_exams.columns:
            exam_tasks_query = exam_tasks[exam_tasks['exam_id'] == exam_id]['task_id']
            recommended_task_ids.extend(exam_tasks_query.tolist())

        # Исключаем выполненные задания
        try:
            conn = sqlite3.connect('your_database.db')
            completed_tasks_query = f"""
                SELECT DISTINCT T.id
                FROM Tasks T
                JOIN Lesson_tasks LT ON T.id = LT.task_id
                JOIN Lessons L ON LT.lesson_id = L.id
                JOIN Schedule_lessons SL ON L.id = SL.lesson_id
                JOIN Schedule S ON SL.schedule_id = S.id
                WHERE S.student_id = {student_id};
            """
            completed_tasks = pd.read_sql_query(completed_tasks_query, conn)
            conn.close()
            if not completed_tasks.empty:
                completed_task_ids = completed_tasks['id'].tolist()
                recommended_task_ids = [tid for tid in recommended_task_ids if tid not in completed_task_ids]
        except sqlite3.Error as e:
            print(f"Ошибка при получении выполненных задач: {e}")

        recommended_tasks = tasks[tasks['id'].isin(recommended_task_ids)]
        return recommended_tasks

    except Exception as e:
        print(f"Ошибка в get_collaborative_recommendations: {e}")
        return pd.DataFrame()


if __name__ == '__main__':
    # Загружаем данные из базы данных
    students, tasks, lessons, lesson_tasks, exam_results, exam_tasks, student_theme_progress = db.load_data()

    if (students is None or tasks is None or lessons is None or
        lesson_tasks is None or exam_results is None or exam_tasks is None or
        student_theme_progress is None):
        print("Ошибка при загрузке данных. Программа завершена.")
    else:
        # Рекомендации на основе содержания (гибридный подход с учетом прогресса)
        student_id_to_recommend = 5
        recommended_content = content_based_recommendations(
            student_id_to_recommend, students, tasks, lessons, lesson_tasks, student_theme_progress)
        print(f"Рекомендации на основе содержания для студента {student_id_to_recommend}")
        print(recommended_content.head())

        # Рекомендации с помощью совместной фильтрации
        model_knn, exam_scores_matrix = collaborative_filtering(exam_results)
        if model_knn is not None and exam_scores_matrix is not None:
            recommended_collab = get_collaborative_recommendations(
                student_id_to_recommend, model_knn, exam_scores_matrix, tasks, exam_tasks)
            print(f"\nРекомендации, полученные с помощью совместной фильтрации для студента {student_id_to_recommend}")
            print(recommended_collab.head())
        else:
            print("Не удалось создать модель совместной фильтрации.")
