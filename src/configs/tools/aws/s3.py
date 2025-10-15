import os  # Importa o módulo para interagir com variáveis de ambiente e o sistema operacional
import boto3  # Importa o SDK da AWS para Python, usado para interagir com serviços AWS, como S3


class AWSS3:
    _instance = None  # Variável de classe para implementar o padrão singleton (uma única instância)

    def __new__(cls, access_key=None, secret_key=None, region_name=None):
        # Sobrescreve o método __new__ para controlar a criação da instância (singleton)
        if cls._instance is None:  # Se ainda não houver uma instância criada
            cls._instance = super().__new__(cls)  # Cria uma nova instância
            # Verifica se as variáveis de ambiente estão configuradas ou se foram passadas credenciais
            if (
                not cls._instance.check_environment_variables()
                and access_key is None
                and secret_key is None
            ):
                # Se não houver credenciais, lança um erro
                raise ValueError("As credenciais da AWS não foram fornecidas.")

            # Define a chave de acesso, priorizando o valor passado ou pegando da variável de ambiente
            cls._instance.access_key = access_key or os.getenv("AWS_ACCESS_KEY_ID")
            # Define a chave secreta, priorizando o valor passado ou pegando da variável de ambiente
            cls._instance.secret_key = secret_key or os.getenv("AWS_SECRET_ACCESS_KEY")
            # Define a região, priorizando o valor passado ou pegando da variável de ambiente
            cls._instance.region_name = region_name or os.getenv("AWS_REGION")

            # Cria o cliente S3 do boto3 usando as credenciais e região definidas
            cls._instance.s3 = boto3.client(
                "s3",
                aws_access_key_id=cls._instance.access_key,
                aws_secret_access_key=cls._instance.secret_key,
                region_name=cls._instance.region_name,
            )
        return cls._instance  # Retorna a instância única

    def download_file_from_s3(self, bucket_name, key, local_file_path):
        # Método para baixar um arquivo do S3
        try:
            # Abre um arquivo local em modo de escrita binária
            with open(local_file_path, "wb") as f:
                # Faz o download do arquivo do S3 para o arquivo local
                self.s3.download_fileobj(bucket_name, key, f)
        except Exception as e:
            # Em caso de erro, lança uma exceção com a mensagem de erro
            raise (f"Erro ao baixar arquivo: {e}")

    def upload_file_to_s3(self, bucket_name, key, local_file_path):
        # Método para enviar um arquivo local para o S3
        try:
            # Envia o arquivo para o S3
            self.s3.upload_file(local_file_path, bucket_name, key)
            return True  # Retorna True se o upload foi bem-sucedido
        except Exception as e:
            # Imprime o erro e retorna False se falhar
            print(e)
            return False

    def delete_file_from_s3(self, bucket_name, key):
        # Método para deletar um arquivo no S3
        try:
            # Deleta o objeto especificado no bucket
            self.s3.delete_object(Bucket=bucket_name, Key=key)
        except Exception as e:
            # Imprime o erro caso ocorra
            print(f"Erro ao deletar arquivo do S3: {e}")

    @staticmethod
    def check_environment_variables():
        # Método estático para verificar se as variáveis de ambiente estão configuradas
        if (
            not os.getenv("AWS_ACCESS_KEY_ID")
            or not os.getenv("AWS_SECRET_ACCESS_KEY")
            or not os.getenv("AWS_REGION")
        ):
            # Se alguma variável não estiver configurada, imprime mensagem e retorna False
            print(
                "As variáveis de ambiente AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY não estão configuradas."
            )
            return False
        else:
            # Se todas as variáveis estiverem configuradas, imprime mensagem e retorna True
            print("Variáveis de ambiente configuradas corretamente.")
            return True
