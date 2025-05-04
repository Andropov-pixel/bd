from db_manager import DBManager
from data_collector import DataCollector
from dotenv import load_dotenv
import os


def setup_database():
    """Инициализация базы данных и создание таблиц."""
    manager.create_tables()


def collect_and_store_data(collector: DataCollector, manager: DBManager):
    """Сбор данных о компаниях и вакансиях и запись их в базу данных."""
    selected_companies_ids = [
        786,  # Яндекс
        1122962,  # Сбер
        15478,  # Mail.Ru Group
        1540,  # РЖД
        1740,  # Газпром нефть
        3529,  # Альфа-Банк
        78636,  # ВТБ Банк
        3776,  # Сбербанк-Технологии
        2180,  # Роснефть
        78653  # Ozon
    ]

    for company_id in selected_companies_ids:
        company_info = collector.get_company(company_id)
        company_name = company_info.get('name')
        manager.insert_company(company_name)

        vacancies = collector.get_vacancies_by_company(company_id)
        for vacancy in vacancies:
            manager.insert_vacancy(
                employer_id=manager.get_company_id(company_name),  # Метод для получения ID компании по имени
                title=vacancy['title'],
                salary_from=vacancy['salary_from'],
                salary_to=vacancy['salary_to'],
                url=vacancy['url'])


if __name__ == "__main__":
    load_dotenv()
    db_host = os.getenv('DB_HOST')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')

    manager = DBManager(db_host, db_name, db_user, db_password)
    collector = DataCollector()

    setup_database()
    collect_and_store_data(collector, manager)

    # Демонстрационные запросы
    print("\nКомпании и количество вакансий:")
    print(manager.get_companies_and_vacancies_count())

    print("\nСредняя зарплата:", manager.get_avg_salary(), "\n")

    print("\nВакансии с зарплатой выше средней:")
    print(manager.get_vacancies_with_higher_salary())

    print("\nВакансии содержащие слово 'Python':")
    print(manager.get_vacancies_with_keyword('Python'))

    manager.close_connection()

    # есть какие-то ошибки