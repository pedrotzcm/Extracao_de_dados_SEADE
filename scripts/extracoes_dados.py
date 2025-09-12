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
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import zipfile
import glob

def obter_conteudo(driver):
    try:
        linhas = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".innerContainer [role='row'][row-index]"))
        )

        dados_temp = set()
        for elemento_linha in linhas:
            celulas = [celula.text.strip() for celula in elemento_linha.find_elements(By.CSS_SELECTOR, "[role='rowheader'], [role='gridcell']")]
            if celulas:
                dados_temp.add(tuple(celulas))
        return dados_temp
    except Exception as e:
        print(f"Erro durante a raspagem do conteúdo: {e}")
        return set()



def rolar_para_mais(driver):
    try:
        linhas = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".innerContainer [role='row'][row-index]"))
        )

        ultimo_elemento_linha = linhas[-1]

        driver.execute_script("arguments[0].scrollIntoView(true);", ultimo_elemento_linha)

        time.sleep(3)

    except Exception as e:
        print(f"Erro durante a rolagem: {e}")
        
        
        


def obter_cabecalhos(driver):
    try:
        elementos_cabecalho = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".top-viewport [role='columnheader']"))
        )
        cabecalhos = [h.text.strip() for h in elementos_cabecalho if h.text.strip()]

        primeiro_cabecalho_coluna = driver.find_element(By.CSS_SELECTOR, ".top-viewport [role='rowheader']")
        if primeiro_cabecalho_coluna and primeiro_cabecalho_coluna.text.strip() not in cabecalhos:
            cabecalhos.insert(0, primeiro_cabecalho_coluna.text.strip())

        return list(dict.fromkeys(cabecalhos))
    except Exception as e:
        print(f"Erro durante a raspagem dos cabeçalhos: {e}")
        return []

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
    
    
    options = get_chrome_options()

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 30)

    try:
        url = 'https://app.powerbi.com/view?r=eyJrIjoiNWI5NWI0ODEtYmZiYy00Mjg3LTkzNWUtY2UyYjIwMDE1YWI2IiwidCI6IjNlYzkyOTY5LTVhNTEtNGYxOC04YWM5LWVmOThmYmFmYTk3OCJ9&pageName=ReportSectionb52b07ec3b5f3ac6c749&pageName=ReportSection'
        driver.get(url)
        driver.implicitly_wait(10)
        time.sleep(5)

        click_grupamento = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="pvExplorationHost"]/div/div/exploration/div/explore-canvas/div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container[9]/transform/div/div[3]/div/div/visual-modern/div/div/div[2]/div')))
        click_grupamento.click()
        time.sleep(2)

        construcao = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[@class='slicerItemContainer' and @title='Construção']")))
        construcao.click()
        time.sleep(2)
        click_grupamento.click()
        time.sleep(2)
        driver.save_screenshot("debug_construcao.png")

        click_UF = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="pvExplorationHost"]/div/div/exploration/div/explore-canvas/div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container[11]/transform/div/div[3]/div/div/visual-modern/div/div/div[2]/div')))
        click_UF.click()
        time.sleep(2)

        acre = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[@class='slicerItemContainer' and @title='Acre']")))
        acre.click()
        driver.save_screenshot("debug1.png")

        time.sleep(2)

        actions = ActionChains(driver)
        found = False

        while not found:
            try:
                sao_paulo = driver.find_element(By.XPATH, "//div[@class='slicerItemContainer' and @title='São Paulo']")
                if(sao_paulo.is_displayed()):
                    found = True
                    driver.save_screenshot("debug2.png")
                    break
            except:
                pass
            actions.send_keys(Keys.ARROW_DOWN).perform()

        if found:
            sao_paulo = driver.find_element(By.XPATH, "//div[@class='slicerItemContainer' and @title='São Paulo']")
            driver.save_screenshot("debug3.png")
            driver.execute_script("arguments[0].click();", sao_paulo)
            time.sleep(2)
            driver.save_screenshot("debug4.png")
            click_UF.click()

        else:
            print("Não foi possível encontrar São Paulo.")

        time.sleep(5)

        chart_xpath = "//*[contains(@aria-label, 'evolucao das admissoes e desligamentos por competencia da movimentacao')] | //*[contains(@title, 'evolucao das admissoes e desligamentos por competencia da movimentacao')] | //div[contains(@class, 'visualContainer') and .//*[contains(text(), 'Admissões')]]"

        grafico = wait.until(EC.visibility_of_element_located((By.XPATH, chart_xpath)))

        actions = ActionChains(driver)
        actions.context_click(grafico).perform()

        time.sleep(2)

        mostrar_como_tabela = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Mostrar como uma tabela'] or .//span[text()='Show as a table']]")))

        mostrar_como_tabela.click()

        time.sleep(5)
        driver.save_screenshot("debug_tabela.png")

    except Exception as e:
        print(f"An error occurred during page setup: {e}")
            


    try:
                #driver = webdriver.Chrome()

                todos_os_dados = set()
                contador = 0
                while True:
                    dados_atuais = obter_conteudo(driver)

                    contagem_dados_originais = len(todos_os_dados)
                    todos_os_dados.update(dados_atuais)

                    driver.save_screenshot(f"debug_{contador}.png")
                    rolar_para_mais(driver)
                    contador += 1

                    if len(todos_os_dados) == contagem_dados_originais:
                        print("Fim da tabela alcançado.")
                        break

                cabecalhos = obter_cabecalhos(driver)
                lista_dados = [list(linha) for linha in todos_os_dados]

                if cabecalhos and lista_dados:
                    max_colunas = len(cabecalhos)
                    dados_processados = []
                    for linha in lista_dados:
                        if len(linha) < max_colunas:
                            dados_processados.append(linha + [None] * (max_colunas - len(linha)))
                        elif len(linha) > max_colunas:
                            dados_processados.append(linha[:max_colunas])
                        else:
                            dados_processados.append(linha)

                    df_table = pd.DataFrame(dados_processados, columns=cabecalhos)
                elif lista_dados:
                    df_table = pd.DataFrame(lista_dados)
                else:
                    df_table = pd.DataFrame()

    except Exception as e:
                print(f"Um erro principal ocorreu: {e}")
                df_table = pd.DataFrame()

    finally:
                    if 'driver' in locals() and driver:
                        driver.quit()
    
    df_table = df_table.rename(columns={0: 'Ano, Mês', 1: 'Admitidos', 2: 'Desligados'})
    df_table['Mes'] = df_table['Ano, Mês'].astype(str).str.split(",").str[1].str.strip()
    df_table['Ano'] = df_table['Ano, Mês'].astype(str).str.split(",").str[0].str.strip() # Extract the year from the 'Ano, Mês' column
    df_table.Admitidos = df_table.Admitidos.astype(str).str.replace(",", "", regex=False).astype(int)
    df_table.Desligados = df_table.Desligados.astype(str).str.replace(",", "", regex=False).astype(int)
    df_table.drop(columns=['Ano, Mês'], inplace=True)
    df_table.to_csv(output_path, index=False)
    print(f"✅ Dados do CAGED salvos em {output_path}")
    return output_path

        
    

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
        
def extrair_anp(output_path):
    """Realiza web scraping para extrair dados da ANP."""
    print("Iniciando web scraping da ANP...")

# Start the driver (copied from s3fMpDIRvwYa for completeness)
    options = get_chrome_options()
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 12) # Increased wait time
    

    url = "https://app.powerbi.com/view?r=eyJrIjoiNzVmNzI1MzQtNTY1NC00ZGVhLTk5N2ItNzBkMDNhY2IxZTIxIiwidCI6IjQ0OTlmNGZmLTI0YTYtNGI0Mi1iN2VmLTEyNGFmY2FkYzkxMyJ9"
    driver.get(url)
    driver.implicitly_wait(10)
    time.sleep(5)

    try:
        click_total = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/report-embed/div/div/div[1]/div/div/div/exploration-container/div/div/docking-container/div/div/div/div/exploration-host/div/div/exploration/div/explore-canvas/div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container-group[2]/transform/div/div[2]/visual-container-group[1]/transform/div/div[2]/visual-container[1]/transform/div/div[3]/div/div/visual-modern/div/div')))
        click_total.click()
        time.sleep(2)
        driver.save_screenshot("tentativa filtros_1.png")

        click_filtros = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="pvExplorationHost"]/div/div/exploration/div/explore-canvas/div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container[1]/transform/div/div[3]/div')))
        click_filtros.click()
        time.sleep(2)
        driver.save_screenshot("tentativa filtros_2.png")

        click_filtros = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="pvExplorationHost"]/div/div/exploration/div/explore-canvas/div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container-group[2]/transform/div/div[2]/visual-container-group[2]/transform/div/div[2]/visual-container-group/transform/div/div[2]/visual-container[6]/transform/div/div[3]/div/div/visual-modern/div/div/div[2]/div')))
        click_filtros.click()
        time.sleep(2)
        driver.save_screenshot("tentativa filtros_3.png")

        actions = ActionChains(driver)
        found = False
        cont = 0

        while not found:
            print("entrando no loop aqui", cont)
            driver.save_screenshot(f"debug_{cont}.png")
            try:
                sao_paulo = driver.find_element(By.XPATH, "//span[text()='São Paulo']")
                is_in_viewport = driver.execute_script("""
                    var elem = arguments[0],
                    box = elem.getBoundingClientRect(),
                    parent = elem.parentElement,
                    parentBox = parent.getBoundingClientRect();

                    while (parent && parent !== document.body) {
                        parentBox = parent.getBoundingClientRect();
                        if (!(box.top <= parentBox.bottom && box.bottom >= parentBox.top &&
                            box.left <= parentBox.right && box.right >= parentBox.left)) {
                            return false;
                        }
                        parent = parent.parentElement;
                    }
                    return box.top >= 0 && box.left >= 0 &&
                        box.bottom <= (window.innerHeight || document.documentElement.clientHeight) &&
                        box.right <= (window.innerWidth || document.documentElement.clientWidth);
                """, sao_paulo)

                if is_in_viewport:
                    driver.save_screenshot(f"achou.png")
                    found = True
                    break
            except:
                cont += 1
                pass
            actions.send_keys(Keys.ARROW_DOWN).perform()
            time.sleep(0.5)

            cont += 1

        if found:
            print("entrando no loop aliii", cont)
            time.sleep(2)
            print(sao_paulo)
            driver.save_screenshot("debugtest.png")
            sao_paulo.click()
            cont += 1

        print("Filtro de 'São Paulo' selecionado com sucesso!")

        click_fecha_filtros = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="pvExplorationHost"]/div/div/exploration/div/explore-canvas/div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container-group[2]/transform/div/div[2]/visual-container[3]/transform/div/div[3]/div/div/visual-modern/div')))
        click_fecha_filtros.click()
        time.sleep(2)

        time.sleep(2)

        chart_xpath = "/html/body/div[1]/report-embed/div/div/div[1]/div/div/div/exploration-container/div/div/docking-container/div/div/div/div/exploration-host/div/div/exploration/div/explore-canvas/div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container[6]/transform/div/div[3]/div"

        grafico = wait.until(EC.visibility_of_element_located((By.XPATH, chart_xpath)))
        actions = ActionChains(driver)
        actions.context_click(grafico).perform()

        time.sleep(2)

        mostrar_como_tabela = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Mostrar como uma tabela'] or .//span[text()='Show as a table']]")))
        
        mostrar_como_tabela.click()
        time.sleep(5)


    except Exception as e:
        print(f"An error occurred: {e}")
        
    try:

        todos_os_dados = set()
        contador = 0
        while True:
            dados_atuais = obter_conteudo(driver)

            contagem_dados_originais = len(todos_os_dados)
            todos_os_dados.update(dados_atuais)

            driver.save_screenshot(f"debug_{contador}.png")
            rolar_para_mais(driver)
            contador += 1

            if len(todos_os_dados) == contagem_dados_originais:
                print("Fim da tabela alcançado.")
                break

        cabecalhos = obter_cabecalhos(driver)
        lista_dados = [list(linha) for linha in todos_os_dados]

        if cabecalhos and lista_dados:
            max_colunas = len(cabecalhos)
            dados_processados = []
            for linha in lista_dados:
                if len(linha) < max_colunas:
                    dados_processados.append(linha + [None] * (max_colunas - len(linha)))
                elif len(linha) > max_colunas:
                    dados_processados.append(linha[:max_colunas])
                else:
                    dados_processados.append(linha)

            df_table = pd.DataFrame(dados_processados, columns=cabecalhos)
        elif lista_dados:
            df_table = pd.DataFrame(lista_dados)
        else:
            df_table = pd.DataFrame()

    except Exception as e:
        print(f"Um erro principal ocorreu: {e}")
        df_table = pd.DataFrame()

    finally:
        
        if 'driver' in locals() and driver:
            driver.quit()
            
        
        
    df_table = df_table.rename(columns={
        0: 'Data',
        1: 'Petróleo (bbl/d)',
        2: 'Petróleo Equivalente (boe/d)',
        3: 'Gás Natural (mil m3/d)',
        4: 'Periodo'
    })


    df_table["Petróleo (bbl/d)"] = df_table["Petróleo (bbl/d)"].astype(str).str.replace(",", "", regex=False).astype(float)
    df_table["Petróleo Equivalente (boe/d)"] = df_table["Petróleo Equivalente (boe/d)"].astype(str).str.replace(",", "", regex=False).astype(float)
    df_table["Gás Natural (mil m3/d)"] = df_table["Gás Natural (mil m3/d)"].astype(str).str.replace(",", "", regex=False).astype(float)

        
    df_table.to_csv(output_path, index=False, encoding='utf-8')
    print(f"✅ Dados da ANP salvos em {output_path}")
    driver.quit()
    return output_path
    
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
        
# falt 