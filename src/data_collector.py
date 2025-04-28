import requests
from typing import List, Dict


class DataCollector:
    """Коллектор данных с сайта hh.ru"""

    def __init__(self):
        self.base_url = 'https://api.hh.ru/'

    def get_company(self, company_id: int) -> Dict[str, any]:
        """ Получение информации о конкретной компании. :param company_id: идентификатор компании :return: словарь с информацией о компании """
        response = requests.get(f'{self.base_url}/employers/{company_id}')
        return response.json() if response.status_code == 200 else {}

    def get_vacancies_by_company(self, company_id: int) -> List[Dict[str, any]]:
        """ Получение списка вакансий определенной компании. :param company_id: идентификатор компании :return: список вакансий компании """
        vacancies = []
        page = 0
        while True:
            params = {'employer_id': company_id, 'page': page}
            response = requests.get(f'{self.base_url}/vacancies', params=params)

            if response.status_code != 200 or not response.json()['items']:
                break

            for item in response.json()['items']:
                vacancy_data = {
                    'id': item['id'],
                    'name': item['name'],
                    'salary_from': item['salary']['from'] if item['salary'] else None,
                    'salary_to': item['salary']['to'] if item['salary'] else None,
                    'url': item['alternate_url']
                }
                vacancies.append(vacancy_data)

            page += 1

        return vacancies