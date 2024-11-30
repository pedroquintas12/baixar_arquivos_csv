import requests

# URL base da API com a consulta SQL
url = "http://dados.recife.pe.gov.br/api/3/action/datastore_search_sql"
query = 'SELECT * from "4adf9430-35a5-4e88-8ecf-b45748b81c7d"'

# Parâmetros da requisição
params = {
    'sql': query
}

try:
    # Fazendo a requisição GET
    response = requests.get(url, params=params)
    
    # Verificando o status da resposta
    if response.status_code == 200:
        # Parseando o resultado para JSON
        data = response.json()
        if 'result' in data:
            records = data['result']['records']
            print("Dados retornados:")
            for record in records:
                print(record)
        else:
            print("Nenhum dado encontrado.")
    else:
        print(f"Erro na requisição: {response.status_code} - {response.text}")
except Exception as e:
    print(f"Erro ao conectar à API: {e}")
