import os
import logging
from flask import Flask, request, render_template_string, send_file
import requests
import csv
from io import StringIO, BytesIO
from bs4 import BeautifulSoup
from pymongo import MongoClient
from dotenv import load_dotenv
import schedule
import time
import threading
import re


# Inicialização do Flask
app = Flask(__name__)

# Configuração de log
logging.basicConfig(level=logging.INFO)

# Carregar variáveis de ambiente
load_dotenv('db.env')

# Configuração do MongoDB
mongo_uri = os.getenv("MONGO_URI")

if not mongo_uri:
    raise ValueError("Certifique-se de que MONGO_URI está configurado.")

client = MongoClient(mongo_uri)

# Lista de URLs monitoradas 
URLS = {}

def generate_unique_id_from_url(url):
    # Expressões regulares para capturar os códigos após 'dataset/' e '/resource/'
    match = re.search(r'dataset/([a-f0-9\-]+).*?/resource/([a-f0-9\-]+)', url)
    if match:
        dataset_code = match.group(1)
        resource_code = match.group(2)
        # Combina os dois códigos para criar um unique_id
        unique_id = f"{dataset_code}_{resource_code}"
        return unique_id
    else:
        logging.error(f"Não foi possível extrair códigos do link: {url}")
        return None
# Função para processar um único CSV
def process_single_csv(page_url, db_collection, link):
    try:
        db_name = "infrações"
        db = client[db_name]
        collection_name = db_collection
        
        # Verifica se a coleção já existe
        if collection_name not in db.list_collection_names():
            # Cria a coleção como capped (limitada a 1 milhão de documentos)
            db.create_collection(
                collection_name, 
                capped=True, 
                size=1024 * 1024 * 500,  # Tamanho máximo em bytes (~500 MB)
                max=1000000  # Limite de 1 milhão de documentos
            )
            logging.info(f"Coleção '{collection_name}' criada com limite de 1 milhão de documentos.")
        
        collection = db[collection_name]
        uniqueid = generate_unique_id_from_url(link)

        # Verifica se o documento já foi baixado pelo unique_id
        if collection.find_one({"unique_id": uniqueid}):
            logging.info(f"Unique ID {uniqueid} já existe no banco de dados '{db_collection}'. Ignorando inserção.")
            return

        # Baixar e processar o CSV
        csv_response = requests.get(link)
        csv_response.raise_for_status()
        csv_data = StringIO(csv_response.text)
        csv_reader = csv.DictReader(csv_data, delimiter=';')  # Por padrão, o separador é ";"

        rows = []
        possible_date_columns = ["data", "datainfracao", "data_infracao", "dataevento"]

        # Limpa os dados para inserir no banco
        for row in csv_reader:
            clean_row = {str(k) if k else "undefined_key": v for k, v in row.items()}

            date_column = next((col for col in possible_date_columns if col in clean_row), None)

            # Se a coluna 'data' existir, destrinchar em ano, mês e dia
            if date_column and clean_row[date_column]:
                # Converter e destrinchar a data em ano, mês e dia
                date_parts = clean_row[date_column].split('-')
                if len(date_parts) == 3:  # Garantir o formato yyyy-mm-dd
                    clean_row["ano"] = date_parts[0]
                    clean_row["mes"] = date_parts[1]
                    clean_row["dia"] = date_parts[2]
                    rows.append(clean_row)

        # Adiciona o source_link e o unique_id no banco
        for row in rows:
            row["source_link"] = page_url
            row["unique_id"] = uniqueid

        if rows:
            collection.insert_many(rows)  # Inserção dos dados
            logging.info(f"{len(rows)} registros inseridos no banco de dados '{db_collection}' para o link {link}.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao acessar o CSV do link {link}: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"Erro ao processar o CSV do link {link}: {e}", exc_info=True)

# Função para processar todos os CSVs (com multithreading)
def process_csvs():
    logging.info("Iniciando o processamento automático de CSVs.")
    threads = []
    csv_processed = False  # Variável para controlar se algum CSV foi processado

    for page_url, db_name in URLS.items():
        try:
            # Baixar o conteúdo da página
            response = requests.get(page_url)
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extrair links para arquivos CSV
            csv_links = [
                tag['href'] for tag in soup.find_all('a', href=True)
                if tag['href'].endswith('.csv')
            ]
            logging.info(csv_links)

            # Verifica se há links CSV na página
            if csv_links:
                csv_processed = True

            for link in csv_links:
                # Criar e iniciar uma nova thread para processar cada CSV
                thread = threading.Thread(target=process_single_csv, args=(page_url, db_name, link))
                thread.start()
                threads.append(thread)

        except requests.exceptions.RequestException as e:
            logging.error(f"Erro ao acessar a página {page_url}: {e}", exc_info=True)
        except Exception as e:
            logging.error(f"Erro ao processar arquivos CSV da página {page_url}: {e}", exc_info=True)

    # Aguardar todas as threads terminarem
    for thread in threads:
        thread.join()

    if not csv_processed:
        logging.info("Nenhum arquivo CSV encontrado para processar.")
    logging.info("Processamento automático de CSVs concluído.")


# Função para extrair o nome do banco de dados do link principal
def extract_db_name_from_link(link):
    db_name = link.split('/')[-1].replace('-', ' ').replace('_', ' ').lower()
    db_name = db_name.replace('.csv', '').replace('dataset', '').strip()
    db_name = db_name.replace(' ', '_')
    return db_name

# Agendamento automático
schedule.every().week.do(process_csvs)

# Interface HTML
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Gerenciador de URLs</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            background-color: #f4f4f9;
            color: #333;
            margin: 0;
            padding: 0;
        }
        .container {
            max-width: 600px;
            margin: 50px auto;
            padding: 20px;
            background: #fff;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        h1 {
            text-align: center;
            color: #444;
        }
        form {
            display: flex;
            justify-content: space-between;
            margin-bottom: 20px;
        }
        input[type="text"] {
            flex: 1;
            padding: 10px;
            margin-right: 10px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        button {
            padding: 10px 15px;
            background: #007BFF;
            color: #fff;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            transition: background 0.3s;
        }
        button:hover {
            background: #0056b3;
        }
        ul {
            list-style: none;
            padding: 0;
        }
        li {
            padding: 10px 15px;
            background: #f9f9f9;
            margin-bottom: 10px;
            border: 1px solid #ddd;
            border-radius: 4px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        li button {
            padding: 5px 10px;
            background: #dc3545;
            color: #fff;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            transition: background 0.3s;
        }
        li button:hover {
            background: #a71d2a;
        }
        li .download {
            background: #28a745;
            color: #fff;
            margin-left: 10px;
        }
        li .download:hover {
            background: #1e7e34;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Gerenciador de URLs</h1>
        <form method="POST" action="/add-url">
            <input type="text" name="url" placeholder="Digite a URL" required>
            <button type="submit">Adicionar</button> 
        </form>
        <h2>URLs Monitoradas</h2>
        <ul>
            {% for url, db_name in urls.items() %}
            <li>
                {{ url }} - Banco: {{ db_name }}
                <form method="POST" action="/remove-url" style="margin: 0; display: inline;">
                    <input type="hidden" name="url" value="{{ url }}">
                    <button type="submit">Remover</button>
                    <a href="/download-csv?url={{ url }}">
                    <button type="button" class="download" style = "background: green">Baixar todos CSV</button>
                    </a>
                </form>
            </li>
            {% endfor %}
        </ul>
    </div>
</body>
</html>
"""

@app.route('/')
def home():
    return render_template_string(HTML_TEMPLATE, urls=URLS)

@app.route('/add-url', methods=['POST'])
def add_url():
    url = request.form.get('url')
    if url and url not in URLS:
        db_name = extract_db_name_from_link(url)
        URLS[url] = db_name
        message = "URL adicionada com sucesso."
    else:
        message = "URL já existe ou é inválida."
    return render_template_string(HTML_TEMPLATE, urls=URLS, message=message)

@app.route('/remove-url', methods=['POST'])
def remove_url():
    url = request.form.get('url')
    if url in URLS:
        del URLS[url]
        message = "URL removida com sucesso."
    else:
        message = "URL não encontrada."
    return render_template_string(HTML_TEMPLATE, urls=URLS, message=message)

@app.route('/download-csv', methods=['GET'])
def download_csv():
    url = request.args.get('url')
    if not url:
        return "URL não fornecida.", 400

    # Aguardar o processamento dos CSVs
    process_csvs()  # Processar todos os CSVs

    # Após o processamento, retornamos a resposta de sucesso
    return "Download realizado com sucesso!", 200

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()
    app.run(debug=True, use_reloader=False)
