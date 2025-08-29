# Arquivo: extratores_dados.py

import pandas as pd
import os
import requests
import openpyxl
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import zipfile
import glob

# --- CONFIGURAÇÕES GERAIS ---
# Mantenha as configurações do WebDriver em um único lugar
def get_chrome_options():
    options = webdriver.ChromeOptions()
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
    options.add_argument(f"user-agent={user_agent}")
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    return options

# --- FUNÇÕES DE EXTRAÇÃO ---

def extrair_ibge_tabela_2296(output_path):
    """Extração dos dados da tabela 2296 (Sinapi) do IBGE."""
    import sidrapy
    print("Iniciando extração da tabela 2296 (Sinapi)...")
    data = sidrapy.get_table(
        table_code="2296",
        territorial_level="3",
        ibge_territorial_code="35",
        period="all"
    )
    data.columns = data.iloc[0]
    df = data[1:].reset_index(drop=True)
    df.to_csv(output_path, index=False)
    print(f"✅ Dados da tabela 2296 salvos em {output_path}")
    return output_path

def extrair_ibge_tabela_8693(output_path):
    """Extração dos dados da tabela 8693 (Serviços) do IBGE."""
    import sidrapy
    print("Iniciando extração da tabela 8693 (Serviços)...")
    data = sidrapy.get_table(
        table_code="8693",
        territorial_level="3",
        ibge_territorial_code="35",
        period="all"
    )
    data.columns = data.iloc[0]
    df = data[1:].reset_index(drop=True)
    df.to_csv(output_path, index=False)
    print(f"✅ Dados da tabela 8693 salvos em {output_path}")
    return output_path

def extrair_ibge_tabela_8888(output_path):
    """Extração dos dados da tabela 8888 (PIMPF) do IBGE."""
    import sidrapy
    print("Iniciando extração da tabela 8888 (PIMPF)...")
    data = sidrapy.get_table(
        table_code="8888",
        territorial_level="3",
        ibge_territorial_code="35",
        variable="12606, 12607",
        classifications={"544": '129314,129315,129316,129317,129318,129320,129321,129324,129326,56689,129330,129331,129332,129333,129334,129335,129336,129337,129338,129339'},
        period="all"
    )
    data.columns = data.iloc[0]
    df = data[1:].reset_index(drop=True)
    df.to_csv(output_path, index=False)
    print(f"✅ Dados da tabela 8888 salvos em {output_path}")
    return output_path

def extrair_ibge_tabela_1086(output_path):
    """Extração dos dados da tabela 1086 (Leite) do IBGE."""
    import sidrapy
    print("Iniciando extração da tabela 1086 (Leite)...")
    data = sidrapy.get_table(
        table_code="1086",
        territorial_level="3",
        ibge_territorial_code="35",
        period="all"
    )
    data.columns = data.iloc[0]
    df = data[1:].reset_index(drop=True)
    df.to_csv(output_path, index=False)
    print(f"✅ Dados da tabela 1086 salvos em {output_path}")
    return output_path
    
def extrair_ibge_tabela_7524(output_path):
    """Extração dos dados da tabela 7524 (Ovos) do IBGE."""
    import sidrapy
    print("Iniciando extração da tabela 7524 (Ovos)...")
    data = sidrapy.get_table(
        table_code="7524",
        territorial_level="3",
        ibge_territorial_code="35",
        period="all"
    )
    data.columns = data.iloc[0]
    df = data[1:].reset_index(drop=True)
    df.to_csv(output_path, index=False)
    print(f"✅ Dados da tabela 7524 salvos em {output_path}")
    return output_path

def extrair_ibge_tabela_647(output_path):
    """Extração dos dados da tabela 647 (Custo de projeto m²) do IBGE."""
    import sidrapy
    print("Iniciando extração da tabela 647 (Custo m²)...")
    data = sidrapy.get_table(
        table_code="647",
        territorial_level="3",
        ibge_territorial_code="35",
        period="all"
    )
    data.columns = data.iloc[0]
    df = data[1:].reset_index(drop=True)
    df.to_csv(output_path, index=False)
    print(f"✅ Dados da tabela 647 salvos em {output_path}")
    return output_path

def extrair_caged_mte(output_path):
    """Baixa o arquivo do CAGED/MTE a partir de uma URL do Google Sheets."""
    print("Iniciando download do arquivo do CAGED/MTE...")
    url_original = 'https://docs.google.com/spreadsheets/d/1QD8BQECbxidvL6t4E4qDRZwRaLDGpkK6/edit?gid=1467329096#gid=1467329096'
    file_id = url_original.split('/d/')[1].split('/')[0]
    url_exportacao = f'https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx'
    
    try:
        response = requests.get(url_exportacao)
        response.raise_for_status()

        with open(output_path, 'wb') as f:
            f.write(response.content)

        print(f"✅ Arquivo '{os.path.basename(output_path)}' salvo com sucesso!")
        return output_path

    except requests.exceptions.RequestException as e:
        print(f"❌ Falha ao baixar o arquivo do CAGED: {e}")
        return None

def extrair_iea(output_path):
    """Realiza web scraping para extrair dados do IEA."""
    print("Iniciando web scraping do IEA...")
    options = get_chrome_options()
    driver = webdriver.Chrome(options=options)
    url = 'https://infoiea.agricultura.sp.gov.br/nia1/Precos_Medios.aspx?cod_sis=3'
    
    try:
        driver.get(url)
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="chkTodos"]'))).click()
        time.sleep(1)
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="imgPesquisar"]'))).click()
        time.sleep(1)
        
        table = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, '//*[@id="Grid"]/tbody')))
        soup = BeautifulSoup(table.get_attribute('innerHTML'), "html.parser")
        
        rows = soup.find_all("tr")
        headers = [td.get_text(strip=True) for td in rows[0].find_all("td")]
        
        data = []
        for row in rows[1:len(rows)-1]:
            cols = [td.get_text(strip=True) for td in row.find_all("td")]
            data.append(cols)
        
        df = pd.DataFrame(data, columns=headers)
        df.to_csv(output_path, index=False, encoding="utf-8")
        print(f"✅ Dados do IEA salvos em {output_path}")
        return output_path
        
    except (TimeoutException, NoSuchElementException) as e:
        print(f"❌ Erro de Selenium na extração do IEA: {e}")
        return None
    finally:
        driver.quit()

def extrair_conab(output_path):
    """Realiza web scraping para extrair dados da CONAB."""
    print("Iniciando web scraping da CONAB...")
    options = get_chrome_options()
    driver = webdriver.Chrome(options=options)
    url = 'https://dw.ceasa.gov.br/'
    
    try:
        driver.get(url)
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="inicial"]/ul/li[1]/a'))).click()
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="tab_panel"]/div/div[2]/div[2]/select'))).click()
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="tab_panel"]/div/div[2]/div[2]/select/optgroup/option[1]'))).click()
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="tab_panel"]/div/div[2]/div[3]/div/div[1]/ul/li/ul/li'))).click()
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="tab_panel"]/div/div[2]/div[3]/div/div[2]/ul/li[1]/span'))).click()
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="tab_panel"]/div/div[2]/div[3]/div/div[2]/ul/li[1]/ul/ul/li[2]/a'))).click()
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="tab_panel"]/div/div[1]/div/div[2]/div/ul/li[2]/a'))).click()
        
        table = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, '//*[@id="table_27"]/table/tbody')))
        soup = BeautifulSoup(table.get_attribute('innerHTML'), "html.parser")
        
        data = []
        header = ["Unidade", "Preço Médio (R$)"]
        
        for row in soup.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) == 1:
                unity = row.find('th').get_text(strip=True)
                price = cells[0].get_text(strip=True)
                data.append([unity, price])
        
        df = pd.DataFrame(data, columns=header)
        df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"✅ Dados da CONAB salvos em {output_path}")
        return output_path
        
    except (TimeoutException, NoSuchElementException) as e:
        print(f"❌ Erro de Selenium na extração da CONAB: {e}")
        return None
    finally:
        driver.quit()

def extrair_bacen(output_path):
    """Realiza web scraping para baixar um arquivo do BACEN."""
    print("Iniciando web scraping do BACEN...")
    options = get_chrome_options()
    
    # Configurar o diretório de download
    download_dir = os.path.dirname(output_path)
    os.makedirs(download_dir, exist_ok=True)
    prefs = {"download.default_directory": download_dir, "download.prompt_for_download": False}
    options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(options=options)
    url = 'https://www.bcb.gov.br/estabilidadefinanceira/balancetesbalancospatrimoniais'
    
    try:
        driver.get(url)
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'ng-select-container')]"))).click()
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'ng-option')][1]"))).click()
        
        download_button = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'btn-download')]")))
        download_button.click()
        
        # Esperar o download ser concluído e encontrar o arquivo
        print("Aguardando o download do arquivo...")
        timeout = 120
        zip_path = None
        while timeout > 0:
            list_of_files = glob.glob(os.path.join(download_dir, '*.zip'))
            if list_of_files:
                zip_path = max(list_of_files, key=os.path.getctime)
                print(f"✅ Arquivo ZIP encontrado: {os.path.basename(zip_path)}")
                break
            time.sleep(1)
            timeout -= 1

        if zip_path:
            # Descompactar e salvar os dados
            unzip_dir = os.path.splitext(output_path)[0] # Usa o nome do arquivo de saida
            os.makedirs(unzip_dir, exist_ok=True)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(unzip_dir)
            
            unzipped_files = glob.glob(os.path.join(unzip_dir, '*.csv'))
            if unzipped_files:
                data_file = unzipped_files[0]
                df = pd.read_csv(data_file, encoding='latin-1', sep=';')
                df.to_csv(output_path, index=False)
                print(f"✅ Dados do BACEN salvos em {output_path}")
            else:
                print("❌ Nenhum arquivo CSV encontrado no zip.")
        else:
            print("❌ Erro: Nenhum arquivo ZIP foi encontrado dentro do tempo limite.")
        return output_path
        
    except (TimeoutException, NoSuchElementException) as e:
        print(f"❌ Erro de Selenium na extração do BACEN: {e}")
        return None
    finally:
        driver.quit()
        
# falta fgv