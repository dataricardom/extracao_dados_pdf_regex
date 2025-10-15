import os
import psycopg2
from sqlalchemy import create_engine


class RDSPostgresSQLManager:
    def __init__(
            self,
            db_name = None,
            db_user = None,
            db_password = None,
            db_host = None,
            db_port = None
    ):
        if (
            not self.check_enviroment_variables
            and db_name is None
            and db_user is None
            and db_password is None
            and db_host is None
            and db_port is None
        ):
            raise ValueError("As credencias do Banco Postgres não foram carregadas!")
        
        self.db_name = db_name or os.getenv("DB_NAME")
        self.db_user = db_user or os.getenv("DB_USER")
        self.db_password = db_password or os.getenv("DB_PASSWORD")
        self.db_host = db_host or os.getenv("DB_HOST")
        self.db_port = db_port or os.getenv("DB_PORT")

    def connect(self):
        try:
            connection = psycopg2.connect(
                dbname = self.db_name,
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port
            )
            print("Conexão bem sucedida com banco POSTGRES!")
            return connection
        except psycopg2.Error as e:
            print(f"Erro ao conectar ao banco POSTGRES: {e}")
            return None

    def execute_query(self, query):
        try:
            connection = self.connect()
            if connection:
                cursor = connection.cursor()
                cursor.execute(query)
                result = cursor.fetchall()
                cursor.close()
                connection.commit()
                connection.close()
                return result
            else:
                print("Não foi possivel estabelecer a conexão com o banco de dados.")
                return None
        except psycopg2.Error as e:
            print(f"Erro ao executar a consulta SQL: {e}")
            return None 
    @staticmethod
    def check_enviroment_variables():
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
        self.engine = create_engine(
            f"postgresql://{self.user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        )

        return self.engine