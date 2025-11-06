ANÁLISE DE INDICADORES DE SAÚDE PÚBLICA COM PERSISTÊNCIA POLIGLOTA
Este repositório contém o projeto de TCC (Trabalho de Conclusão de Curso) intitulado "ANÁLISE DE INDICADORES DE SAÚDE PÚBLICA A PARTIR DE DADOS DO PORTAL DADOS.GOV.BR, COM FOCO NA PERSISTÊNCIA POLIGLOTA EM BANCOS DE DADOS NOSQL E RELACIONAIS".

O projeto consiste em um pipeline de ETL-A (Extração, Transformação, Carga e Análise) robusto, desenvolvido em Python e Pandas , que processa dados públicos de saúde (Dengue/SINAN) e os persiste em uma arquitetura poliglota, utilizando MySQL e MongoDB para diferentes propósitos analíticos.





Autores:

Paulo Henrique Dantas Teodosio 

Wesley Gabriel Teixeira de Aragão 

Orientador:

Prof. Israel da Costa Cunha 

🏛️ Arquitetura da Solução
O núcleo deste projeto é a demonstração prática da persistência poliglota . Os dados não são apenas armazenados, mas transformados e direcionados para o banco de dados mais adequado para a tarefa analítica.

O pipeline processa 1.502.259 registros de notificações de Dengue de 2024.



Fonte de Dados: dados_dengue.json (1.4 GB, 121 colunas).


Motor ETL-A (Python/Pandas):

Extração: Leitura do JSON.


Análise (EDA): Análise de qualidade dos dados (ex: descoberta de 99,2% de dados de idade ausentes).



Mapeamento: Seleção e renomeação de ~25 colunas de interesse.


Enriquecimento: Junção (merge) com municipios.csv para adicionar nome_municipio e codigo_uf.

Transformação: Limpeza, decodificação (ex: NU_IDADE_N) e bifurcação da lógica de persistência.

Destino 1: MongoDB (Banco NoSQL)


Propósito: Repositório de Data Science e análise exploratória.



Modelagem: Os dados são salvos "planos" (ex: sint_febre: '1'), preservando a granularidade para consultas flexíveis e de perfil clínico .


Destino 2: MySQL (Banco Relacional)


Propósito: Data Mart Analítico para BI e reporting.



Modelagem: Múltiplas colunas de sintomas (ex: FEBRE, VOMITO) são agregadas em um único campo do tipo JSON (sintomas_json) , otimizando o banco para consultas agregadas (GROUP BY) e relatórios.


🚀 Principais Features e Desafios Resolvidos

Processamento em Lote (Batch Processing): A carga de 1.5M de registros no MySQL falhou com o Erro 2055 (max_allowed_packet) . O pipeline resolve isso implementando a inserção em lotes de 50.000 registros, garantindo a carga completa .



Enriquecimento de Dados: Traduz códigos brutos (ID_MUNICIP) em dados legíveis (nome_municipio), essencial para a análise de indicadores.

Decodificação de Dados: Converte dados complexos do DataSUS (ex: NU_IDADE_N '1035' -> 35 anos) em formatos analíticos padronizados.

Pipeline Reexecutável: Graças ao script preparar_mysql.py, a arquitetura pode ser destruída e recriada de forma consistente.

🛠️ Stack Tecnológico
Linguagem: Python 3.x

Processamento de Dados: Pandas, NumPy

Banco de Dados Relacional: MySQL

Banco de Dados NoSQL: MongoDB

Conectores Python: mysql-connector-python, pymongo

⚙️ Configuração e Instalação
Clone o repositório:

Bash
-------------------------------------------------------------
git clone https://github.com/seu-usuario/seu-repositorio.git
cd seu-repositorio
Instale as dependências:
-------------------------------------------------------------
Bash
-------------------------------------------------------
pip install pandas mysql-connector-python pymongo requests
(É recomendado o uso de um ambiente virtual venv)
-------------------------------------------------------
Pré-requisitos:

Tenha instâncias locais do MySQL e MongoDB em execução.

Crie um banco de dados no MySQL chamado tcc_dengue.

Atualize as credenciais de banco de dados (usuário, senha) no topo do carregar_dados.py e preparar_mysql.py.

Dados:

Coloque o arquivo de dados do SINAN na pasta raiz com o nome: dados_dengue.json.

(Opcional) O script baixar_csv.py pode ser usado para baixar o arquivo municipios.csv, ou você pode baixá-lo manualmente.

▶️ Como Executar o Pipeline
A execução deve seguir esta ordem:

Passo 1: Preparar o Banco de Dados MySQL Este script cria (ou recria) a tabela notificacoes no MySQL com o esquema correto, incluindo as colunas JSON e os campos de enriquecimento.

Bash
-----------------------------
python preparar_mysql.py
-----------------------------
Passo 2: (Opcional) Baixar o CSV de Municípios Se o municipios.csv não estiver na pasta, este script irá baixá-lo.

Bash
---------------------
python baixar_csv.py
---------------------
Passo 3: Executar o Pipeline Principal (ETL-A) Este é o script principal. Ele fará todo o processo: ler o JSON, analisar, transformar, enriquecer e carregar os dados no MongoDB e no MySQL.

Bash
------------------------
python carregar_dados.py
------------------------

Saída Esperada: O terminal exibirá o progresso, incluindo a Análise Exploratória (EDA) e o log de carregamento dos 31 lotes no MySQL, finalizando com:

...
[Top 10 Municípios por Notificação (Nome)]
nome_municipio
São Paulo                64604
São José do Rio Preto    58881
Porto Alegre             55224
...

Conectando ao MongoDB...
Sucesso! 1502259 documentos inseridos no MongoDB.
------------------------------
Conectando ao MySQL...
Transformando dados para SQL (agrupando JSONs)...
...
Enviando lote 31 para o MySQL (1500001 a 1502259)...
Sucesso! Todos os 1502259 registros foram inseridos no MySQL.
