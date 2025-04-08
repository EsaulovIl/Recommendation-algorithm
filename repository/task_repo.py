# repository/task_repo.py
import pandas as pd
from db import get_connection

def get_completed_task_ids(student_id):
    with get_connection() as conn:
        query = f"""
            SELECT DISTINCT T.id
            FROM Tasks T
            JOIN Lesson_tasks LT ON T.id = LT.task_id
            JOIN Lessons L ON LT.lesson_id = L.id
            JOIN Schedule_lessons SL ON L.id = SL.lesson_id
            JOIN Schedule S ON SL.schedule_id = S.id
            WHERE S.student_id = {student_id}
        """
        df = pd.read_sql_query(query, conn)
        return df['id'].tolist() if not df.empty else []

def get_tasks_with_themes():
    with get_connection() as conn:
        return pd.read_sql_query('''
            SELECT T.id, T.section_id, T.description, T.complexity, T.theme_id, th.name AS theme_name
            FROM Tasks T
            JOIN Themes th ON T.theme_id = th.id
        ''', conn)

def get_themes():
    with get_connection() as conn:
        return pd.read_sql_query("SELECT id AS theme_id, name AS theme_name FROM Themes", conn)
