import mysql.connector
from mysql.connector import errorcode

# --- Configuração do MySQL ---
MYSQL_CONFIG = {
    'user': 'root',
    'password': 'Teo1205.', # Sua senha
    'host': '127.0.0.1',
    'database': 'tcc_dengue'
}

DB_NAME = 'tcc_dengue'

TABLES = {}
TABLES['notificacoes'] = (
    "CREATE TABLE `notificacoes` ("
    "  `id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `data_notificacao` date DEFAULT NULL,"
    "  `municipio_ibge` varchar(10) DEFAULT NULL,"
    "  `sexo` char(1) DEFAULT NULL,"
    "  `idade_anos` float DEFAULT NULL," 
    "  `criterio_confirmacao` varchar(100) DEFAULT NULL,"
    "  `evolucao` varchar(100) DEFAULT NULL,"
    "  `sintomas_json` JSON DEFAULT NULL,"
    "  `comorbidades_json` JSON DEFAULT NULL,"
    "  `nome_municipio` VARCHAR(255) DEFAULT NULL,"
    # --- MUDANÇA AQUI ---
    "  `codigo_uf` INT(2) DEFAULT NULL,"                # MUDOU DE 'uf CHAR(2)'
    # --- FIM DA MUDANÇA ---
    "  PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB"
)

def criar_banco_e_tabelas():
    try:
        cnx = mysql.connector.connect(
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            host=MYSQL_CONFIG['host']
        )
        cursor = cnx.cursor()
        
        try:
            cursor.execute(f"CREATE DATABASE {DB_NAME} DEFAULT CHARACTER SET 'utf8'")
            print(f"Banco de dados '{DB_NAME}' criado com sucesso.")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_DB_CREATE_EXISTS:
                print(f"Banco de dados '{DB_NAME}' já existe.")
            else:
                print(err)
                exit(1)
        
        cnx.database = DB_NAME

        try:
            print("Tentando deletar tabela 'notificacoes' antiga...")
            cursor.execute("DROP TABLE IF EXISTS notificacoes")
            print("Tabela antiga deletada (se existia).")
        except mysql.connector.Error as err:
            print(f"Erro ao deletar tabela: {err}")

        # Cria a nova tabela
        for table_name in TABLES:
            table_description = TABLES[table_name]
            try:
                print(f"Criando tabela '{table_name}'... ", end='')
                cursor.execute(table_description)
                print("OK")
            except mysql.connector.Error as err:
                print(err.msg)

        cursor.close()
        cnx.close()

    except mysql.connector.Error as err:
        print(f"Erro ao conectar ou configurar o MySQL: {err}")
        exit(1)

if __name__ == "__main__":
    criar_banco_e_tabelas()