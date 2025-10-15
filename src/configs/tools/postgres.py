import os  # Para acessar variáveis de ambiente
import psycopg2  # Para conectar e interagir com bancos Postgres usando psycopg2
from sqlalchemy import create_engine  # Para criar engines SQLAlchemy para interagir com o banco

class RDSPostgresSQLManager:
    def __init__(
            self,
            db_name = None,
            db_user = None,
            db_password = None,
            db_host = None,
            db_port = None
    ):
        # Verifica se as variáveis de ambiente estão configuradas ou se foram passados parâmetros
        if (
            not self.check_enviroment_variables
            and db_name is None
            and db_user is None
            and db_password is None
            and db_host is None
            and db_port is None
        ):
            # Lança erro se nenhuma credencial foi fornecida
            raise ValueError("As credencias do Banco Postgres não foram carregadas!")
        
        # Define os parâmetros do banco, priorizando valores passados ou variáveis de ambiente
        self.db_name = db_name or os.getenv("DB_NAME")
        self.db_user = db_user or os.getenv("DB_USER")
        self.db_password = db_password or os.getenv("DB_PASSWORD")
        self.db_host = db_host or os.getenv("DB_HOST")
        self.db_port = db_port or os.getenv("DB_PORT")

    def connect(self):
        # Método para criar conexão direta com o Postgres via psycopg2
        try:
            connection = psycopg2.connect(
                dbname = self.db_name,
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port
            )
            print("Conexão bem sucedida com banco POSTGRES!")
            return connection  # Retorna a conexão estabelecida
        except psycopg2.Error as e:
            # Captura erro caso a conexão falhe
            print(f"Erro ao conectar ao banco POSTGRES: {e}")
            return None

    def execute_query(self, query):
        # Método para executar uma query SQL no banco
        try:
            connection = self.connect()  # Cria a conexão
            if connection:
                cursor = connection.cursor()  # Cria cursor para executar comandos
                cursor.execute(query)  # Executa a query
                result = cursor.fetchall()  # Busca todos os resultados
                cursor.close()  # Fecha o cursor
                connection.commit()  # Aplica alterações no banco
                connection.close()  # Fecha a conexão
                return result  # Retorna os resultados da consulta
            else:
                print("Não foi possivel estabelecer a conexão com o banco de dados.")
                return None
        except psycopg2.Error as e:
            # Captura erro caso a execução da query falhe
            print(f"Erro ao executar a consulta SQL: {e}")
            return None 

    @staticmethod
    def check_enviroment_variables():
        # Método estático para verificar se as variáveis de ambiente do banco estão configuradas
        if (
            not os.getenv("DB_NAME")
            or not os.getenv("DB_USER")
            or not os.getenv("DB_PASSWORD")
            or not os.getenv("DB_HOST")
        ):
            print("As variáveis de ambiente do banco não estão configuradas.")
            return False
        else:
            print("Variáveis de ambiente para o banco foram configuradas corretamente!")
            return True

    def alchemy(self):
        # Cria um engine do SQLAlchemy para operações mais avançadas e integração com Pandas
        self.engine = create_engine(
            f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        )

        return self.engine  # Retorna o engine criado
