# repository/student_repo.py
import pandas as pd
from db import get_connection

def get_student_by_id(student_id):
    with get_connection() as conn:
        return pd.read_sql_query(
            f"SELECT * FROM Students WHERE id = {student_id}", conn
        )
