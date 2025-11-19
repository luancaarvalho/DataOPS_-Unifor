import requests
from datetime import date, timedelta

#cria função para extrair dados do api
def gerar_hoje():
##extrai o dia de hoje e a previão dos próximos 3 dias
    hoje = []

    url_hoje = 'http://api.weatherapi.com/v1/forecast.json?key=0c8f2e5c12764c82878182905250911&q=Fortaleza&days=3&aqi=no&alerts=no'
    response = requests.get(url_hoje)
    if response.status_code == 200:
        data = response.json()
        hoje.append(data)
    else:
        print(f"Erro: {response.status_code}")
    
    return hoje


def gerar_historico():
#extrai o histórico de 3 dias
    dias_historico = []

    for i in range(1,4):
        dias = (date.today() - timedelta(days=i)).isoformat()
        dias_historico.append(dias)

    historico = []

    for dia in dias_historico:
        url_historico = f'http://api.weatherapi.com/v1/history.json?key=0c8f2e5c12764c82878182905250911&q=Fortaleza&dt={dia}'
        response = requests.get(url_historico)

        if response.status_code == 200:
            data = response.json()
            historico.append(data)
        else:
            print(f"Erro no {dia}: {response.status_code}")

    return historico

if __name__ == "__main__":
    historico = gerar_historico()
    print(historico)
    hoje = gerar_hoje()
    print(hoje)