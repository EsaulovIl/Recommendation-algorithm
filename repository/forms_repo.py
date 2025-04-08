# repository/forms_repo.py
import pandas as pd
from db import get_connection

def get_student_preferences(student_id):
    with get_connection() as conn:
        query = f"SELECT preferences FROM Forms WHERE student_id = {student_id}"
        df = pd.read_sql_query(query, conn)
        return df['preferences'].iloc[0].split(', ') if not df.empty else []
