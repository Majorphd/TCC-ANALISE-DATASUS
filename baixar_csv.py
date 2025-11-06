import requests
import pandas as pd
import os

# URL "crua" (Raw) do arquivo CSV, que aponta para o dado puro
url_correta = "https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/main/csv/municipios.csv"
nome_arquivo = "municipios.csv"
pasta_tcc = r"c:\temp\TCC" # Onde seu script está
caminho_completo = os.path.join(pasta_tcc, nome_arquivo)

print(f"Iniciando o download do arquivo CSV correto de:\n{url_correta}\n")

try:
    # 1. Deleta o arquivo corrompido (se existir)
    if os.path.exists(caminho_completo):
        print(f"Arquivo '{nome_arquivo}' corrompido encontrado. Deletando...")
        os.remove(caminho_completo)
        print("Arquivo antigo deletado.")

    # 2. Baixa o novo arquivo
    response = requests.get(url_correta)
    response.raise_for_status() # Verifica se há erros (como 404 ou 429)

    # 3. Salva o arquivo no local correto
    with open(caminho_completo, 'wb') as f:
        f.write(response.content)
    
    print(f"\nSucesso! Arquivo '{nome_arquivo}' baixado corretamente em '{caminho_completo}'")

    # 4. Teste de verificação
    print("\nVerificando o arquivo baixado com o Pandas...")
    df_teste = pd.read_csv(caminho_completo)
    print("Verificação do Pandas bem-sucedida! Colunas encontradas:")
    print(df_teste.columns.tolist())
    print("\nInício do arquivo:")
    print(df_teste.head())
    print("\n>>> TUDO PRONTO! <<<")
    print(">>> AGORA VOCÊ PODE RODAR O 'carregar_dados.py' <<<")


except requests.exceptions.RequestException as e:
    print(f"\nERRO DE DOWNLOAD: Não foi possível baixar o arquivo.")
    print(f"Detalhe: {e}")
except pd.errors.ParserError as e:
    print(f"\nERRO DE LEITURA: O arquivo baixado ainda está corrompido.")
    print(f"Detalhe: {e}")
except Exception as e:
    print(f"\nUM ERRO INESPERADO OCORREU: {e}")