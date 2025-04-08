# validator.py
import logging

def validate_tasks(tasks):
    errors = []
    if tasks['complexity'].isnull().any():
        errors.append("Есть задачи без указания сложности.")
    if not tasks['complexity'].between(1, 10).all():
        errors.append("Есть задачи с некорректной сложностью (не от 1 до 10).")
    return errors

def validate_forms(forms, students):
    errors = []
    if forms['preferences'].isnull().any():
        errors.append("Есть анкеты без предпочтений.")
    missing_students = forms[~forms['student_id'].isin(students['id'])]
    if not missing_students.empty:
        errors.append(f"Формы с несуществующими student_id: {missing_students['student_id'].tolist()}")
    return errors

def validate_progress(progress):
    errors = []
    if not progress['progress'].between(0, 100).all():
        errors.append("Есть значения прогресса вне диапазона 0–100.")
    duplicated = progress.duplicated(subset=['student_id', 'theme_id']).sum()
    if duplicated:
        errors.append(f"Найдено дублирующихся записей по student_id + theme_id: {duplicated}")
    return errors

def run_all(tasks, forms, students, progress):
    all_errors = []
    all_errors.extend(validate_tasks(tasks))
    all_errors.extend(validate_forms(forms, students))
    all_errors.extend(validate_progress(progress))

    if all_errors:
        for err in all_errors:
            logging.warning(f"[VALIDATION] {err}")
            print(f"⚠️  {err}")
    else:
        print("Данные прошли валидацию успешно.")