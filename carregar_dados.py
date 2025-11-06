import json
import pandas as pd
import mysql.connector
from pymongo import MongoClient
import numpy as np # Importante para tratar nulos

# --- Configurações (Idênticas) ---
MONGO_CONFIG = {
    'connection_string': 'mongodb://localhost:27017/',
    'database': 'tcc_dengue',
    'collection': 'notificacoes'
}
MYSQL_CONFIG = {
    'user': 'root',
    'password': 'Teo1205.',
    'host': '127.0.0.1',
    'database': 'tcc_dengue'
}
JSON_FILE_PATH = 'dados_dengue.json' 


# Mapeamento de colunas de interesse (Nome no DataSUS: Nosso Nome)
MAPEAMENTO_COLUNAS = {
    'DT_NOTIFIC': 'data_notificacao',
    'ID_MUNICIP': 'municipio_ibge',
    'NU_IDADE_N': 'idade_codigo', # <--- Nome temporário, vamos processar
    'CS_SEXO':    'sexo',
    'CRITERIO':   'criterio_confirmacao',
    'EVOLUCAO':   'evolucao',
    'HOSPITALIZ': 'hospitalizacao',
    'DT_OBITO':   'data_obito',
    
    # Mapeando sintomas (1=Sim, 2=Não, 9=Ignorado)
    'FEBRE':      'sint_febre',
    'MIALGIA':    'sint_mialgia',
    'CEFALEIA':   'sint_cefaleia',
    'EXANTEMA':   'sint_exantema',
    'VOMITO':     'sint_vomito',
    'NAUSEA':     'sint_nausea',
    'DOR_COSTAS': 'sint_dor_costas',
    'ARTRALGIA':  'sint_artralgia',
    'DOR_RETRO':  'sint_dor_retro',
    
    # Mapeando comorbidades (1=Sim, 2=Não, 9=Ignorado)
    'DIABETES':   'como_diabetes',
    'HEMATOLOG':  'como_hematologica',
    'HEPATOPAT':  'como_hepatopatia',
    'RENAL':      'como_renal',
    'HIPERTENSA': 'como_hipertensao',
    'AUTO_IMUNE': 'como_autoimune'
}

# (NOVO) Função para decodificar a idade do DataSUS
def parse_idade_datasus(codigo_idade):
    """
    Decodifica o campo NU_IDADE_N (string de 4 chars).
    1º char: 1=Anos, 2=Meses, 3=Dias.
    3 últimos chars: O valor.
    Ex: '1035' = 35 anos. '2006' = 6 meses.
    Retorna a idade em anos (int).
    """
    if not isinstance(codigo_idade, str) or len(codigo_idade) != 4:
        return None # Retorna Nulo se o dado estiver mal formatado
    
    try:
        tipo = codigo_idade[0]
        valor = int(codigo_idade[1:])
        
        if tipo == '1': # Anos
            return valor
        elif tipo == '2': # Meses
            return valor / 12
        elif tipo == '3': # Dias
            return valor / 365
        else: # Ignora outros tipos (horas, minutos)
            return None
    except (ValueError, TypeError):
        return None # Retorna Nulo se 'valor' não for numérico

# (NOVO) Função para criar JSON de sintomas/comorbidades
def criar_json_agrupado(row, prefixo, mapa_nomes):
    """
    Recebe uma linha (row) do DataFrame, um prefixo (ex: 'sint_')
    e um mapa (ex: {'sint_febre': 'Febre'}).
    Retorna um JSON string de sintomas/comorbidades onde o valor foi '1' (Sim).
    """
    itens_presentes = []
    for col_original, nome_limpo in mapa_nomes.items():
        # Verifica se a coluna existe e se o valor é 1 (Sim)
        if col_original in row and row[col_original] == '1':
            itens_presentes.append(nome_limpo)
            
    if not itens_presentes:
        return None
    
    return json.dumps(itens_presentes, ensure_ascii=False)


def carregar_dados_mongodb(df):
    """
    Carrega os dados (já mapeados) no MongoDB.
    """
    try:
        print("Conectando ao MongoDB...")
        client = MongoClient(MONGO_CONFIG['connection_string'])
        db = client[MONGO_CONFIG['database']]
        collection = db[MONGO_CONFIG['collection']]
        
        collection.delete_many({})
        
        # Converte NaN/NaT do pandas para None (Nulo)
        df_mongo = df.replace({np.nan: None, pd.NaT: None})
        
        # Converte o DataFrame limpo em uma lista de dicionários
        # O Mongo vai salvar os dados já "semi-limpos" (mapeados)
        dados = df_mongo.to_dict('records')
        
        result = collection.insert_many(dados)
        
        print(f"Sucesso! {len(result.inserted_ids)} documentos inseridos no MongoDB.")
        client.close()
        
    except Exception as e:
        print(f"Erro ao carregar no MongoDB: {e}")


def transformar_e_carregar_mysql(df):
    """
    (VERSÃO CORRIGIDA 2.0)
    Transforma os dados do DataFrame e os carrega no MySQL.
    """
    try:
        print("Conectando ao MySQL...")
        cnx = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = cnx.cursor()
        
        cursor.execute("TRUNCATE TABLE notificacoes")

        # --- Fase de Transformação (O 'T' do ETL) ---
        df_sql = df.copy()

        print("Transformando dados para SQL (agrupando JSONs)...")
        mapa_sintomas = {
            'sint_febre': 'Febre', 'sint_mialgia': 'Mialgia', 'sint_cefaleia': 'Cefaleia',
            'sint_exantema': 'Exantema', 'sint_vomito': 'Vômito', 'sint_nausea': 'Náusea',
            'sint_dor_costas': 'Dor nas Costas', 'sint_artralgia': 'Artralgia', 
            'sint_dor_retro': 'Dor Retro-orbital'
        }
        mapa_comorbidades = {
            'como_diabetes': 'Diabetes', 'como_hematologica': 'Doença Hematológica',
            'como_hepatopatia': 'Hepatopatia', 'como_renal': 'Doença Renal',
            'como_hipertensao': 'Hipertensão', 'como_autoimune': 'Doença Autoimune'
        }
        df_sql['sintomas_json'] = df_sql.apply(
            lambda row: criar_json_agrupado(row, 'sint_', mapa_sintomas), axis=1
        )
        df_sql['comorbidades_json'] = df_sql.apply(
            lambda row: criar_json_agrupado(row, 'como_', mapa_comorbidades), axis=1
        )

        df_sql.rename(columns={'idade_anos': 'idade_anos'}, inplace=True)
        
        # --- MUDANÇA AQUI ---
        colunas_finais_sql = [
            'data_notificacao', 'municipio_ibge', 'sexo', 'idade_anos',
            'criterio_confirmacao', 'evolucao', 
            'sintomas_json', 'comorbidades_json',
            'nome_municipio', 'codigo_uf'  # MUDOU DE 'uf'
        ]
        # --- FIM DA MUDANÇA ---
        
        df_sql_final = df_sql[colunas_finais_sql].copy()

        df_sql_final = df_sql_final.replace({np.nan: None, pd.NaT: None})
        
        print("Convertendo DataFrame final para lista de dicionários...")
        registros_para_inserir = df_sql_final.to_dict('records')
        
        print(f"Total de {len(registros_para_inserir)} registros prontos para o MySQL.")

        # --- Fase de Carga (O 'L' do ETL) ---
        
        # --- MUDANÇA AQUI (Query SQL) ---
        query = (
            "INSERT INTO notificacoes "
            "(data_notificacao, municipio_ibge, sexo, idade_anos, "
            "criterio_confirmacao, evolucao, sintomas_json, comorbidades_json, "
            "nome_municipio, codigo_uf) " # MUDOU DE 'uf'
            "VALUES (%(data_notificacao)s, %(municipio_ibge)s, %(sexo)s, %(idade_anos)s, "
            "%(criterio_confirmacao)s, %(evolucao)s, %(sintomas_json)s, %(comorbidades_json)s, "
            "%(nome_municipio)s, %(codigo_uf)s)" # MUDOU DE '%(uf)s'
        )
        # --- FIM DA MUDANÇA ---

        BATCH_SIZE = 50000
        total_registros = len(registros_para_inserir)
        
        for i in range(0, total_registros, BATCH_SIZE):
            fim_lote = min(i + BATCH_SIZE, total_registros)
            lote_atual = registros_para_inserir[i:fim_lote]
            
            print(f"Enviando lote {i // BATCH_SIZE + 1} para o MySQL ({i+1} a {fim_lote})...")
            
            cursor.executemany(query, lote_atual)
            cnx.commit()
        
        print(f"Sucesso! Todos os {total_registros} registros foram inseridos no MySQL.")
        
        cursor.close()
        cnx.close()

    except Exception as e:
        print(f"Erro ao carregar no MySQL: {e}")

def main():
    # --- Fase de Extração (O 'E' do ETL) com PANDAS ---
    try:
        colunas_reais = list(MAPEAMENTO_COLUNAS.keys())
        dtype_map = {col: 'object' for col in colunas_reais}
        print(f"Iniciando leitura do JSON. Isso pode levar um tempo para 1.4GB...")
        df_completo = pd.read_json(JSON_FILE_PATH, dtype=dtype_map)
        df_filtrado = df_completo[colunas_reais].copy()
        df_mapeado = df_filtrado.rename(columns=MAPEAMENTO_COLUNAS)
        
        print(f"Arquivo '{JSON_FILE_PATH}' lido e filtrado com sucesso. {len(df_mapeado)} registros encontrados.")
        
    except FileNotFoundError:
        print(f"Erro: Arquivo '{JSON_FILE_PATH}' não encontrado.")
        return
    except KeyError as e:
        print(f"Erro de Mapeamento: A coluna {e} não foi encontrada no JSON.")
        return
    except Exception as e:
        print(f"Erro ao ler o JSON com pandas: {e}")
        return
    
    # --- (CORRIGIDO) Fase de Enriquecimento de Dados (O 'T' do ETL) ---
    print("\nIniciando enriquecimento com nomes de municípios...")
    try:
        df_municipios = pd.read_csv('municipios.csv')
        
        df_mapeado['municipio_ibge'] = df_mapeado['municipio_ibge'].astype(str)
        df_municipios['codigo_ibge'] = df_municipios['codigo_ibge'].astype(str) 

        df_municipios['codigo_6_digitos'] = df_municipios['codigo_ibge'].str[:6]

        df_enriquecido = pd.merge(
            df_mapeado,
            # --- MUDANÇA AQUI ---
            df_municipios[['codigo_6_digitos', 'nome', 'codigo_uf']], # MUDOU DE 'uf'
            # --- FIM DA MUDANÇA ---
            left_on='municipio_ibge',
            right_on='codigo_6_digitos',
            how='left'
        )
        
        df_enriquecido = df_enriquecido.rename(columns={'nome': 'nome_municipio'})
        
    except FileNotFoundError:
        print(f"ERRO: Arquivo 'municipios.csv' não encontrado. As análises seguirão com o código IBGE.")
        df_enriquecido = df_mapeado 
        df_enriquecido['nome_municipio'] = None
        # --- MUDANÇA AQUI ---
        df_enriquecido['codigo_uf'] = None # MUDOU DE 'uf'
        # --- FIM DA MUDANÇA ---

    print("Enriquecimento de municípios concluído.")

    # --- Fase de Transformação e Análise (Pandas) ---
    print("\n" + "="*40)
    print(" ANÁLISE EXPLORATÓRIA PÓS-MAPEAMENTO E ENRIQUECIMENTO")
    print("="*40)
    
    print("\nProcessando 'NU_IDADE_N' (idade_codigo)...")
    df_enriquecido['idade_anos'] = df_enriquecido['idade_codigo'].apply(parse_idade_datasus)
    
    print("\n[Estatísticas Descritivas (Idade em Anos)]")
    print(df_enriquecido['idade_anos'].describe())
    
    print("\nProcessando Datas...")
    df_enriquecido['data_notificacao'] = pd.to_datetime(df_enriquecido['data_notificacao'], errors='coerce')

    print("\n[Contagem de Casos por Sexo (CS_SEXO)]")
    print(df_enriquecido['sexo'].value_counts())

    print("\n[Contagem de Evolução dos Casos (EVOLUCAO)]")
    print(df_enriquecido['evolucao'].value_counts(dropna=False))

    print("\n[Top 10 Municípios por Notificação (Nome)]")
    print(df_enriquecido['nome_municipio'].value_counts().head(10))
    
    print("="*40 + "\n")
    
    # --- Carga para os dois bancos ---
    carregar_dados_mongodb(df_enriquecido)
    print("-" * 30)
    transformar_e_carregar_mysql(df_enriquecido)

if __name__ == "__main__":
    main()