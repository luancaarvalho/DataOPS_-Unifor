import requests
from tenacity import retry, stop_after_attempt, wait_fixed


class ApiHandler:
    
    def __init__(self, url=None):
        self.url = url or 'https://s3.amazonaws.com/aml-sample-data/banking.csv'
    
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def _consult_api(self):
        try:
            response = requests.get(self.url, stream=True)
            response.raise_for_status()
            return response
        
        except requests.RequestException as err:
            raise Exception(f'Não foi possível consultar a API - ({err})')
    
    def get_lines(self):
        try:
            with self._consult_api() as response:
                response.raise_for_status()
                yield from (line.decode('utf-8') for line in response.iter_lines())

        except Exception as err:
            return Exception(f'Não foi possivel consultar a API - ({err})')