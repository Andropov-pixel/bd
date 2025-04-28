import psycopg2
from typing import Optional, List, Tuple, Any


class DBManager:
    def __init__(self, host: str, database: str, user: str, password: str):
        try:
            self.conn = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password.encode('utf-8'),  # Преобразование пароля в байтовый объект
                options='-c client_encoding=UTF8'
            )
            print("Подключение к базе данных успешно установлено.")
        except Exception as e:
            raise RuntimeError(f'Ошибка подключения к базе данных: {e}')

    def close_connection(self):
        """Закрывает соединение с базой данных."""
        if hasattr(self, 'conn'):
            self.conn.close()

    def create_tables(self):
        """Создание необходимых таблиц в базе данных."""
        with self.conn.cursor() as cur:
            # Создаем таблицу компаний
            cur.execute(
                """ CREATE TABLE IF NOT EXISTS companies ( id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL UNIQUE ); """)

            # Создаем таблицу вакансий
            cur.execute(
                """ CREATE TABLE IF NOT EXISTS vacancies ( id SERIAL PRIMARY KEY, employer_id INTEGER REFERENCES companies(id), title VARCHAR(255) NOT NULL, salary_from FLOAT DEFAULT NULL, salary_to FLOAT DEFAULT NULL, url TEXT NOT NULL ); """)
        self.conn.commit()

    def insert_company(self, company_name: str) -> bool:
        """Вставка новой компании в базу данных."""
        with self.conn.cursor() as cur:
            cur.execute("INSERT INTO companies(name) VALUES (%s)", (company_name,))
        self.conn.commit()
        return True

    def insert_vacancy(self, employer_id: int, title: str, salary_from: float, salary_to: float, url: str) -> bool:
        """Вставка новой вакансии в базу данных."""
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO vacancies(employer_id, title, salary_from, salary_to, url) VALUES (%s, %s, %s, %s, %s)",
                (employer_id, title, salary_from, salary_to, url))
        self.conn.commit()
        return True

    def get_companies_and_vacancies_count(self) -> List[Tuple[int, str, int]]:
        """Возвращает список компаний и количество вакансий у каждой."""
        with self.conn.cursor() as cur:
            cur.execute(
                """ SELECT c.id, c.name, COUNT(v.id) AS vacancies_count FROM companies c LEFT JOIN vacancies v ON c.id = v.employer_id GROUP BY c.id, c.name; """)
            rows = cur.fetchall()
        return [(row[0], row[1], row[2]) for row in rows]

    def get_all_vacancies(self) -> list[tuple[Any, Any, Any, Any, Any]]:
        """Возвращает список всех вакансий с именем компании, названием вакансии, зарплатой и ссылкой."""
        with self.conn.cursor() as cur:
            cur.execute(
                """ SELECT c.name, v.title, v.salary_from, v.salary_to, v.url FROM vacancies v INNER JOIN companies c ON v.employer_id = c.id; """)
            rows = cur.fetchall()
        return [(row[0], row[1], row[2], row[3], row[4]) for row in rows]

    def get_avg_salary(self) -> Optional[float]:
        """Средняя зарплата по всем вакансиям."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT AVG((v.salary_from + v.salary_to)/2) FROM vacancies v;")
            result = cur.fetchone()[0]
        return round(result, 2) if result is not None else None

    def get_vacancies_with_higher_salary(self) -> list[Any] | list[tuple[Any, Any, Any, Any, Any]]:
        """Список вакансий с зарплатой выше средней."""
        avg_salary = self.get_avg_salary()
        if avg_salary is None:
            return []

        with self.conn.cursor() as cur:
            cur.execute(
                """ SELECT c.name, v.title, v.salary_from, v.salary_to, v.url FROM vacancies v INNER JOIN companies c ON v.employer_id = c.id WHERE ((v.salary_from+v.salary_to)/2 > %s); """,
                (avg_salary,))
            rows = cur.fetchall()
        return [(row[0], row[1], row[2], row[3], row[4]) for row in rows]

    def get_vacancies_with_keyword(self, keyword: str) -> list[tuple[Any, Any, Any, Any, Any]]:
        """Поиск вакансий по ключевым словам в названии."""
        with self.conn.cursor() as cur:
            cur.execute(
                """ SELECT c.name, v.title, v.salary_from, v.salary_to, v.url FROM vacancies v INNER JOIN companies c ON v.employer_id = c.id WHERE LOWER(v.title) LIKE LOWER(%s); """,
                ('%' + keyword + '%',))
            rows = cur.fetchall()
        return [(row[0], row[1], row[2], row[3], row[4]) for row in rows]

    def get_company_id(self, company_name):
        pass

    # есть какие-то ошибки