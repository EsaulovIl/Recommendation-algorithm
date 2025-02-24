import sqlite3
import pandas as pd
import datetime
import random

def create_data():
    """Создает базу данных с улучшенной структурой и заполняет её данными."""
    conn = sqlite3.connect('your_database.db')
    cursor = conn.cursor()
    try:
        # Таблицы для преподавателей, курсов, разделов и т.п.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Teacher (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                phone_number TEXT,
                email TEXT
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Course (
                id INTEGER PRIMARY KEY,
                description TEXT
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Section (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                description TEXT
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Course_section (
                course_id INTEGER,
                section_id INTEGER,
                PRIMARY KEY (course_id, section_id),
                FOREIGN KEY (course_id) REFERENCES Course(id),
                FOREIGN KEY (section_id) REFERENCES Section(id)
            );
        ''')

        # Избегаем зарезервированного слова, переименовывая таблицу Group в StudentGroup
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS StudentGroup (
                id INTEGER PRIMARY KEY,
                mentor_id INTEGER,
                course_id INTEGER,
                FOREIGN KEY (mentor_id) REFERENCES Mentor(id),
                FOREIGN KEY (course_id) REFERENCES Course(id)
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Mentor (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Schedule (
                id INTEGER PRIMARY KEY,
                student_id INTEGER,
                FOREIGN KEY (student_id) REFERENCES Students(id)
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Task_interaction (
                task_id INTEGER,
                type TEXT,
                field TEXT,
                PRIMARY KEY (task_id, type),
                FOREIGN KEY (task_id) REFERENCES Tasks(id)
            );
        ''')

        # Улучшенная таблица Themes – добавлены дополнительные поля
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Themes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                complexity INTEGER NOT NULL,
                priority INTEGER,
                prerequisites TEXT,
                recommended_resources TEXT,
                section_id INTEGER,
                FOREIGN KEY (section_id) REFERENCES Section(id)
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Tasks (
                id INTEGER PRIMARY KEY,
                section_id INTEGER,
                description TEXT NOT NULL,
                complexity INTEGER NOT NULL,
                theme_id INTEGER,
                FOREIGN KEY (theme_id) REFERENCES Themes(id)
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Lessons (
                id INTEGER PRIMARY KEY,
                section_id INTEGER,
                FOREIGN KEY (section_id) REFERENCES Section(id)
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Lesson_themes (
                lesson_id INTEGER,
                theme_id INTEGER,
                percentage REAL NOT NULL,
                PRIMARY KEY (lesson_id, theme_id),
                FOREIGN KEY (lesson_id) REFERENCES Lessons(id),
                FOREIGN KEY (theme_id) REFERENCES Themes(id)
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Lesson_tasks (
                lesson_id INTEGER,
                task_id INTEGER,
                grade INTEGER NOT NULL,
                PRIMARY KEY (lesson_id, task_id),
                FOREIGN KEY (lesson_id) REFERENCES Lessons(id),
                FOREIGN KEY (task_id) REFERENCES Tasks(id)
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Exams (
                id INTEGER PRIMARY KEY
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Exam_tasks (
                exam_id INTEGER,
                task_id INTEGER,
                PRIMARY KEY (exam_id, task_id),
                FOREIGN KEY (exam_id) REFERENCES Exams(id),
                FOREIGN KEY (task_id) REFERENCES Tasks(id)
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Exam_results (
                id INTEGER PRIMARY KEY,
                exam_id INTEGER,
                student_id INTEGER,
                grade INTEGER NOT NULL,
                FOREIGN KEY (exam_id) REFERENCES Exams(id)
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Schedule_lessons (
                schedule_id INTEGER,
                lesson_id INTEGER,
                deadline DATE NOT NULL,
                percentage REAL NOT NULL,
                PRIMARY KEY (schedule_id, lesson_id),
                FOREIGN KEY (lesson_id) REFERENCES Lessons(id)
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Forms (
                id INTEGER PRIMARY KEY,
                preferences TEXT NOT NULL,
                target_score INTEGER NOT NULL,
                additional_exam TEXT,
                student_id INTEGER,
                FOREIGN KEY (student_id) REFERENCES Students(id)
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Students (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                phone_number TEXT NOT NULL,
                email TEXT NOT NULL,
                group_id INTEGER,
                course_id INTEGER,
                FOREIGN KEY (group_id) REFERENCES StudentGroup(id),
                FOREIGN KEY (course_id) REFERENCES Course(id)
            );
        ''')

        # Новая таблица для отслеживания прогресса по темам у студентов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS StudentThemeProgress (
                student_id INTEGER,
                theme_id INTEGER,
                progress REAL,  -- процент освоения темы от 0 до 100
                last_updated DATE,
                PRIMARY KEY (student_id, theme_id),
                FOREIGN KEY (student_id) REFERENCES Students(id),
                FOREIGN KEY (theme_id) REFERENCES Themes(id)
            );
        ''')

        conn.commit()

    except sqlite3.Error as e:
        print(f"Ошибка базы данных: {e}")
    finally:
        conn.close()

def get_complexity(theme_name, section_id):
    """
    Пример функции, вычисляющей сложность (1..10) на основе:
    1) Ключевых слов в названии темы
    2) Номера раздела (section_id): чем больше, тем выше сложность

    Вы можете при желании настроить ключевые слова и формулу.
    """
    base = 1
    lower_name = theme_name.lower()

    # Ключевые слова
    if "неравен" in lower_name:
        base += 2
    if "логарифм" in lower_name:
        base += 2
    if "треугольн" in lower_name:
        base += 2
    if "тригонометр" in lower_name:
        base += 3
    if "комбинатор" in lower_name:
        base += 3
    if "параметр" in lower_name:
        base += 2
    if "вероятн" in lower_name:
        base += 2

    # Учитываем номер раздела (section_id)
    # Простая формула: + (section_id // 3)
    # или что-то ещё
    base += section_id // 3

    # Обрезаем до макс. 10
    if base < 1:
        base = 1
    if base > 10:
        base = 10

    return base

def insert_data():
    conn = sqlite3.connect('your_database.db')
    cursor = conn.cursor()

    cursor.executemany("INSERT OR IGNORE INTO Teacher (id, name, phone_number, email) VALUES (?, ?, ?, ?)", [
        (1, 'Андрей Смирнов', '1111111111', 'andrey@example.com'),
        (2, 'Екатерина Иванова', '2222222222', 'ekaterina@example.com'),
        (3, 'Сергей Волков', '3333333333', 'sergey@example.com'),
        (4, 'Ольга Дмитриева', '4444444444', 'olga@example.com'),
        (5, 'Николай Сидоров', '5555555555', 'nikolay@example.com'),
        (6, 'Марина Ковалева', '6666666666', 'marina@example.com'),
        (7, 'Игорь Чернов', '7777777777', 'igor@example.com')
    ])

    cursor.executemany("INSERT OR IGNORE INTO Course (id, description) VALUES (?, ?)", [
        (1, 'Курс 0 → 40 баллов'),
        (2, 'Курс 0 → 70 баллов'),
        (3, 'Курс 0 → 86 баллов'),
        (4, 'Курс 0 → 100 баллов')
    ])

    cursor.executemany("INSERT OR IGNORE INTO Section (id, description) VALUES (?, ?)", [
        (1, 'Планиметрия'),
        (2, 'Векторы'),
        (3, 'Стереометрия'),
        (4, 'Начала теории вероятностей'),
        (5, 'Вероятности сложных событий'),
        (6, 'Простейшие уравнения'),
        (7, 'Вычисления и преобразования'),
        (8, 'Производная и первообразная'),
        (9, 'Задачи с прикладным содержанием'),
        (10, 'Текстовые задачи'),
        (11, 'Графики функций'),
        (12, 'Наибольшее и наименьшее значение функций'),
        (13, 'Уравнения'),
        (14, 'Стереометрическая задача'),
        (15, 'Неравенства'),
        (16, 'Финансовая математика'),
        (17, 'Планиметрическая задача'),
        (18, 'Задача с параметром'),
        (19, 'Числа и их свойства')
    ])

    cursor.executemany("INSERT OR IGNORE INTO Course_section (course_id, section_id) VALUES (?, ?)", [
        (1, 2),
        (1, 4),
        (1, 5),
        (1, 6),
        (1, 7),
        (1, 9),
        (1, 11),
        (2, 1),
        (2, 2),
        (2, 3),
        (2, 4),
        (2, 5),
        (2, 6),
        (2, 7),
        (2, 8),
        (2, 9),
        (2, 10),
        (2, 11),
        (2, 12),
        (3, 1),
        (3, 2),
        (3, 3),
        (3, 4),
        (3, 5),
        (3, 6),
        (3, 7),
        (3, 8),
        (3, 9),
        (3, 10),
        (3, 11),
        (3, 12),
        (3, 13),
        (3, 15),
        (3, 16),
        (4, 1),
        (4, 2),
        (4, 3),
        (4, 4),
        (4, 5),
        (4, 6),
        (4, 7),
        (4, 8),
        (4, 9),
        (4, 10),
        (4, 11),
        (4, 12),
        (4, 13),
        (4, 14),
        (4, 15),
        (4, 16),
        (4, 17),
        (4, 18),
        (4, 19)
    ])

    cursor.executemany("INSERT OR IGNORE INTO Mentor (id, name) VALUES (?, ?)", [
        (1, 'Иван Петров'),
        (2, 'Мария Козлова'),
        (3, 'Александр Смирнов'),
        (4, 'Ольга Никитина')
    ])

    cursor.executemany("INSERT OR IGNORE INTO StudentGroup (id, mentor_id, course_id) VALUES (?, ?, ?)", [
        (1, 1, 1),
        (2, 2, 2),
        (3, 3, 3),
        (4, 4, 4),
    ])

    cursor.executemany("INSERT OR IGNORE INTO Schedule (id, student_id) VALUES (?, ?)", [
        (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)
    ])

    cursor.executemany("INSERT OR IGNORE INTO Task_interaction (task_id, type, field) VALUES (?, ?, ?)", [
        (1, 'просмотр', 'дата'),
        (2, 'решение', 'ответ'),
        (3, 'обсуждение', 'комментарий'),
        (4, 'проверка', 'оценка'),
        (5, 'повторное решение', 'дата')
    ])

    # Вставка данных в Themes с дополнительными полями
    themes_raw = [
            (1, "Решение прямоугольного треугольника", 1),
            (2, "Решение равнобедренного треугольника", 1),
            (3, "Треугольники общего вида", 1),
            (4, "Параллелограммы", 1),
            (5, "Трапеция", 1),
            (6, "Центральные и вписанные углы", 1),
            (7, "Касательная, хорда, секущая", 1),
            (8, "Вписанные окружности", 1),
            (9, "Описанные окружности", 1),

            # --- Раздел 2. Векторы ---
            (10, "Векторы и операции с ними", 2),

            # --- Раздел 3. Стереометрия ---
            (11, "Куб", 3),
            (12, "Прямоугольный параллелепипед", 3),
            (13, "Элементы составных многогранников", 3),
            (14, "Площадь поверхности составного многогранника", 3),
            (15, "Объем составного многогранника", 3),
            (16, "Призма", 3),
            (17, "Пирамида", 3),
            (18, "Комбинации тел", 3),
            (19, "Цилиндр", 3),
            (20, "Конус", 3),
            (21, "Шар", 3),

            # --- Раздел 4. Начала теории вероятностей ---
            (22, "Классическое определение вероятности", 4),

            # --- Раздел 5. Вероятности сложных событий (section_id=5) ---
            (23, "Теоремы о вероятностях событий", 5),
            (24, "Новые задания банка MathЕГЭ", 5),

            # --- Раздел 6. Простейшие уравнения (section_id=6) ---
            (25, "Линейные, квадратные, кубические уравнения", 6),
            (26, "Рациональные уравнения", 6),
            (27, "Иррациональные уравнения", 6),
            (28, "Показательные уравнения", 6),
            (29, "Логарифмические уравнения", 6),
            (30, "Тригонометрические уравнения", 6),

            # --- Раздел 7. Вычисления и преобразования (section_id=7) ---
            (31, "Преобразования числовых рациональных выражений", 7),
            (32, "Преобразования алгебраических выражений и дробей", 7),
            (33, "Вычисление значений степенных выражений", 7),
            (34, "Действия со степенями", 7),
            (35, "Преобразования числовых иррациональных выражений", 7),
            (36, "Преобразования буквенных иррациональных выражений", 7),
            (37, "Преобразования числовых логарифмических выражений", 7),
            (38, "Преобразования буквенных логарифмических выражений", 7),
            (39, "Вычисление значений тригонометрических выражений", 7),
            (40, "Преобразования числовых тригонометрических выражений", 7),
            (41, "Преобразования буквенных тригонометрических выражений", 7),

            # --- Раздел 8. Производная и первообразная ---
            (42, "Физический смысл производной", 8),
            (43, "Геометрический смысл производной, касательная", 8),
            (44, "Применение производной к исследованию функций", 8),
            (45, "Первообразная", 8),

            # --- Раздел 9. Задачи с прикладным содержанием ---
            (46, "Линейные уравнения и неравенства", 9),
            (47, "Квадратные и степенные уравнения и неравенства", 9),
            (48, "Рациональные уравнения и неравенства", 9),
            (49, "Иррациональные уравнения и неравенства", 9),
            (50, "Показательные уравнения и неравенства", 9),
            (51, "Логарифмические уравнения и неравенства", 9),
            (52, "Тригонометрические уравнения и неравенства", 9),
            (53, "Разные задачи", 9),

            # --- Раздел 10. Текстовые задачи ---
            (54, "Задачи на проценты, сплавы и смеси", 10),
            (55, "Задачи на движение по прямой", 10),
            (56, "Задачи на движение по окружности", 10),
            (57, "Задачи на движение по воде", 10),
            (58, "Задачи на совместную работу", 10),
            (59, "Задачи на прогрессии", 10),

            # --- Раздел 11. Графики функций (section_id=11) ---
            (60, "Линейные функции", 11),
            (61, "Параболы", 11),
            (62, "Гиперболы", 11),
            (63, "Корни", 11),
            (64, "Показательные и логарифмические функции", 11),
            (65, "Тригонометрические функции", 11),
            (66, "Комбинированные задачи", 11),

            # --- Раздел 12. Наибольшее и наименьшее значение функций (section_id=12) ---
            (67, "Исследование функции без помощи производной", 12),
            (68, "Исследование степенных и иррациональных функций", 12),
            (69, "Исследование частных", 12),
            (70, "Исследование произведений", 12),
            (71, "Исследование показательных и логарифмических функций", 12),
            (72, "Исследование тригонометрических функций", 12),

            (73, "Показательные уравнения", 13),
            (74, "Рациональные уравнения", 13),
            (75, "Иррациональные уравнения", 13),
            (76, "Логарифмические уравнения", 13),
            (77, "Тригонометрические уравнения, сводимые к квадратными", 13),
            (78, "Тригонометрические уравнения, сводимые к однородным", 13),
            (79, "Тригонометрические уравнения, разложение на множители", 13),
            (80, "Тригонометрические уравнения, исследование ОДЗ", 13),
            (81, "Тригонометрические уравнения, разные задачи", 13),
            (82, "Тригонометрия и иррациональности", 13),
            (83, "Тригонометрия и логарифмы", 13),
            (84, "Тригонометрия и показательные выражения", 13),
            (85, "Другие уравнения смешанного типа", 13),

            (86, "Расстояние между прямыми и плоскостями", 14),
            (87, "Расстояние от точки до прямой", 14),
            (88, "Расстояние от точки до плоскости", 14),
            (89, "Сечения пирамид", 14),
            (90, "Сечения призм", 14),
            (91, "Сечения параллелепипедов", 14),
            (92, "Угол между плоскостями", 14),
            (93, "Угол между плоскостями граней многогранника", 14),
            (94, "Угол между прямой и плоскостью", 14),
            (95, "Угол между скрещивающимися прямыми", 14),
            (96, "Объём многогранников", 14),
            (97, "Сечения круглых тел", 14),
            (98, "Круглые тела: цилиндр, конус, шар", 14),
            (99, "Комбинации фигур", 14),

            (100, "Неравенства, содержащие радикалы", 15),
            (101, "Рациональные неравенства", 15),
            (102, "Показательные неравенства", 15),
            (103, "Неравенства рациональные относительно показательной функции", 15),
            (104, "Логарифмические неравенства первой и второй степени", 15),
            (105, "Неравенства рациональные относительно логарифмической функции", 15),
            (106, "Неравенства с логарифмами по переменному основанию", 15),
            (107, "Неравенства с логарифмами по переменному основанию, применение рационализации", 15),
            (108, "Логарифмические неравенства, разные задачи", 15),
            (109, "Показательные выражения и иррациональности", 15),
            (110, "Логарифмы и показательные выражения", 15),
            (111, "Логарифмы и иррациональности", 15),
            (112, "Неравенства с тригонометрией", 15),
            (113, "Неравенства с модулем", 15),
            (114, "Другие неравенства смешанного типа", 15),

            (115, "Вклады", 16),
            (116, "Кредиты", 16),
            (117, "Задачи на оптимальный выбор", 16),
            (118, "Разные задачи", 16),

            (119, "Треугольники и их свойства", 17),
            (120, "Четырёхугольники и их свойства", 17),
            (121, "Окружности и системы окружностей", 17),
            (122, "Вписанные окружности и треугольники", 17),
            (123, "Описание окружности и треугольник", 17),
            (124, "Окружности и треугольники, разные задачи", 17),
            (125, "Вписанные окружности и четырехугольники", 17),
            (126, "Описанные окружности и четырехугольники", 17),
            (127, "Окружности и четырёхугольники, разные задачи", 17),
            (128, "Разные задачи о многоугольниках", 17),

            (129, "Уравнения с параметром", 18),
            (130, "Уравнения с параметром, содержащие модуль", 18),
            (131, "Уравнения с параметром, содержащие радикалы", 18),
            (132, "Неравенства с параметром", 18),
            (133, "Системы с параметром", 18),
            (134, "Расположение корней квадратного трехчлена", 18),
            (135, "Использование симметрий", 18),
            (136, "Использование монотонности, оценок", 18),
            (137, "Аналитическое решение уравнений и неравенств", 18),
            (138, "Аналитическое решение систем", 18),
            (139, "Координаты (x, a)", 18),
            (140, "Уравнение окружности", 18),
            (141, "Расстояние между точками", 18),
            (142, "Функции, зависящие от параметра", 18),

            (143, "Числа и их свойства", 19),
            (144, "Числовые наборы на карточках и досках", 19),
            (145, "Последовательности и прогрессии", 19),
            (146, "Сюжетные задачи: кино, театр, мотки верёвки", 19)
        ]

    new_themes = []
    for (tid, tname, sec_id) in themes_raw:
        comp = get_complexity(tname, sec_id)
        new_themes.append((tid, tname, comp, sec_id))

    # Теперь вставляем их в Themes
    cursor.executemany(
        "INSERT OR IGNORE INTO Themes (id, name, complexity, section_id) VALUES (?, ?, ?, ?)",
        new_themes
    )

    cursor.executemany("INSERT OR IGNORE INTO Lessons (id, section_id) VALUES (?, ?)", [
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 4),
        (5, 5)
    ])

    cursor.executemany("INSERT OR IGNORE INTO Lesson_themes (lesson_id, theme_id, percentage) VALUES (?, ?, ?)", [
        (1, 1, 60.0),
        (2, 2, 50.0),
        (3, 3, 70.0),
        (4, 4, 80.0),
        (5, 5, 90.0)
    ])

    cursor.executemany("INSERT OR IGNORE INTO Lesson_tasks (lesson_id, task_id, grade) VALUES (?, ?, ?)", [
        (1, 1, 85),
        (2, 2, 90),
        (3, 3, 75),
        (4, 4, 80),
        (5, 5, 95)
    ])

    cursor.executemany("INSERT OR IGNORE INTO Exams (id) VALUES (?)", [
        (1,), (2,), (3,), (4,), (5,)
    ])

    cursor.executemany("INSERT OR IGNORE INTO Exam_tasks (exam_id, task_id) VALUES (?, ?)", [
        (1, 1),
        (1, 2),
        (2, 3),
        (2, 4),
        (3, 5)
    ])

    cursor.executemany("INSERT OR IGNORE INTO Exam_results (id, exam_id, student_id, grade) VALUES (?, ?, ?, ?)", [
        (1, 1, 1, 85),
        (2, 2, 2, 90),
        (3, 3, 3, 78),
        (4, 4, 4, 88),
        (5, 5, 5, 92)
    ])

    cursor.executemany(
        "INSERT OR IGNORE INTO Schedule_lessons (schedule_id, lesson_id, deadline, percentage) VALUES (?, ?, ?, ?)", [
            (1, 1, '2025-03-01', 80.0),
            (1, 2, '2025-03-05', 85.0),
            (2, 3, '2025-03-10', 90.0),
            (2, 4, '2025-03-15', 75.0),
            (3, 5, '2025-03-20', 95.0)
        ])

    # Обновленная вставка данных в Forms:
    # Для каждого студента (id от 1 до 20) генерируем форму с предпочтениями из тем.
    # Предпочтения выбираются случайно: 2–4 темы из списка имен тем.
    cursor.execute("SELECT id, name FROM Themes")
    themes = cursor.fetchall()  # список кортежей (theme_id, name)
    theme_names = [t[1] for t in themes]

    forms = []
    for student_id in range(1, 21):
        num_prefs = random.randint(2, 4)
        prefs = random.sample(theme_names, num_prefs)
        prefs_str = ", ".join(prefs)
        target_score = random.randint(70, 100)
        additional_exams = ["Физика", "Информатика", "Химия", "Биология", "Экономика"]
        additional_exam = random.choice(additional_exams)
        # Используем student_id как id формы (или можно auto increment, если поле id настроено как PRIMARY KEY AUTOINCREMENT)
        forms.append((student_id, prefs_str, target_score, additional_exam, student_id))

    cursor.executemany(
        "INSERT OR IGNORE INTO Forms (id, preferences, target_score, additional_exam, student_id) VALUES (?, ?, ?, ?, ?)",
        forms
    )

    cursor.executemany(
        "INSERT OR IGNORE INTO Students (id, name, phone_number, email, group_id, course_id) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, 'Иван Иванов', '1234567890', 'ivan@example.com', 1, 1),
            (2, 'Мария Петрова', '0987654321', 'maria@example.com', 2, 2),
            (3, 'Петр Сидоров', '1122334455', 'petr@example.com', 3, 3),
            (4, 'Анна Смирнова', '5566778899', 'anna@example.com', 4, 4),
            (5, 'Олег Козлов', '6677889900', 'oleg@example.com', 1, 1),
            (6, 'Алексей Сидоров', '1231231234', 'aleksei@example.com', 1, 1),
            (7, 'Елена Петрова', '2342342345', 'elena@example.com', 2, 2),
            (8, 'Дмитрий Иванов', '3453453456', 'dmitry@example.com', 3, 3),
            (9, 'Наталья Смирнова', '4564564567', 'natalya@example.com', 4, 4),
            (10, 'Сергей Козлов', '5675675678', 'sergey@example.com', 2, 2),
            (11, 'Марина Васильева', '6786786789', 'marina.v@example.com', 1, 1),
            (12, 'Владимир Соколов', '7897897890', 'vladimir@example.com', 2, 2),
            (13, 'Ольга Федорова', '8908908901', 'olga.f@example.com', 3, 3),
            (14, 'Константин Григорьев', '9019019012', 'konstantin@example.com', 4, 4),
            (15, 'Татьяна Соколова', '0120120123', 'tatiana@example.com', 3, 3),
            (16, 'Андрей Миронов', '1112223334', 'andrey.m@example.com', 1, 1),
            (17, 'Екатерина Кузнецова', '2223334445', 'ekaterina.k@example.com', 2, 2),
            (18, 'Максим Попов', '3334445556', 'maksim@example.com', 3, 3),
            (19, 'Светлана Лебедева', '4445556667', 'svetlana@example.com', 4, 4),
            (20, 'Роман Фролов', '5556667778', 'roman@example.com', 4, 4)
        ]
    )

    # Вставка данных в StudentThemeProgress (прогресс по темам)
    today = datetime.date.today().isoformat()
    cursor.executemany(
        "INSERT OR IGNORE INTO StudentThemeProgress (student_id, theme_id, progress, last_updated) VALUES (?, ?, ?, ?)",
        [
            (1, 1, 80.0, today),
            (1, 2, 75.0, today),
            (2, 1, 70.0, today),
            (2, 3, 65.0, today),
            (3, 2, 60.0, today),
            (4, 4, 85.0, today),
            (5, 4, 50.0, today),  # Для студента 5 низкий прогресс по теме "Вероятность"
            (5, 5, 55.0, today)  # Для студента 5 низкий прогресс по теме "Комбинаторика"
        ])
    conn.commit()
    conn.close()

def load_data():
    conn = sqlite3.connect('your_database.db')
    # Загружаем данные в DataFrame
    students = pd.read_sql_query("SELECT * FROM Students", conn)
    tasks = pd.read_sql_query("SELECT * FROM Tasks", conn)
    lessons = pd.read_sql_query("SELECT * FROM Lessons", conn)
    lesson_tasks = pd.read_sql_query("SELECT * FROM Lesson_tasks", conn)
    exam_results = pd.read_sql_query("SELECT * FROM Exam_results", conn)
    exam_tasks = pd.read_sql_query("SELECT * FROM Exam_tasks", conn)
    student_theme_progress = pd.read_sql_query("SELECT * FROM StudentThemeProgress", conn)
    cursor = conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_students_id ON Students (id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tasks_section_id ON Tasks (section_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_lesson_tasks_lesson_id ON Lesson_tasks (lesson_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_lesson_tasks_task_id ON Lesson_tasks (task_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tasks_theme_id ON Tasks (theme_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_exams_student_id ON Exam_results (student_id)")
    conn.commit()
    conn.close()
    return students, tasks, lessons, lesson_tasks, exam_results, exam_tasks, student_theme_progress


if __name__ == '__main__':
    create_data()
    insert_data()



def get_connection():
    return sqlite3.connect('your_database.db')
