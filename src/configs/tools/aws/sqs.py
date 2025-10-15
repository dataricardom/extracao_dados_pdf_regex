import os  # Importa o módulo para interagir com variáveis de ambiente e sistema operacional
import boto3  # Importa o SDK da AWS para Python, usado para interagir com serviços AWS, neste caso SQS


class AWSSQSManager:
    def __init__(
        self,
        access_key: str = None,
        secret_key: str = None,
        region_name: str = "us-east-2",  # Define a região padrão caso não seja informada
    ):
        # Verifica se as variáveis de ambiente estão configuradas ou se foram passadas credenciais
        if (
            not self.check_environment_variables()
            and access_key is None
            and secret_key is None
        ):
            # Se não houver credenciais, lança um erro
            raise ValueError("As credenciais da AWS não foram fornecidas.")

        # Define a chave de acesso, priorizando o valor passado ou pegando da variável de ambiente
        self.access_key = access_key or os.getenv("AWS_ACCESS_KEY_ID")
        # Define a chave secreta, priorizando o valor passado ou pegando da variável de ambiente
        self.secret_key = secret_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        # Define a região, priorizando o valor passado ou pegando da variável de ambiente
        self.region_name = region_name or os.getenv("AWS_REGION")

        # Verifica novamente se as credenciais estão definidas
        if not self.access_key or not self.secret_key:
            raise ValueError("As credenciais da AWS não foram fornecidas.")

        # Cria o cliente SQS do boto3 usando as credenciais e região definidas
        self.sqs = boto3.client(
            "sqs",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region_name,
        )

    def get_queue_url(self, queue_name):
        # Método para obter a URL de uma fila SQS pelo nome
        try:
            response = self.sqs.get_queue_url(QueueName=queue_name)
            return response["QueueUrl"]  # Retorna a URL da fila
        except Exception as e:
            # Imprime erro caso não consiga obter a URL
            print(f"Erro ao obter URL da fila: {e}")
            return None

    def receive_messages_from_queue(
        self,
        queue_name: str,
        max_number_of_messages: int = 10,  # Número máximo de mensagens a serem recebidas
        visibility_timeout: int = 30,  # Tempo que a mensagem ficará invisível para outras requisições
    ):
        # Método para receber mensagens de uma fila
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.get_queue_url(queue_name),  # Obtém a URL da fila
                MaxNumberOfMessages=max_number_of_messages,  # Define quantidade máxima
                VisibilityTimeout=visibility_timeout,  # Define tempo de invisibilidade
                WaitTimeSeconds=0,  # Tempo de espera para long polling (0 = curto)
            )
            messages = response.get("Messages", [])  # Pega a lista de mensagens ou retorna lista vazia
            return messages
        except Exception as e:
            # Imprime erro caso falhe ao receber mensagens
            print(f"Erro ao receber mensagens da fila: {e}")
            return []

    def check_message_in_queue(self, queue_name: str):
        # Método para verificar se há mensagens na fila
        try:
            response = self.sqs.get_queue_attributes(
                QueueUrl=self.get_queue_url(queue_name),  # Obtém a URL da fila
                AttributeNames=["ApproximateNumberOfMessages"],  # Solicita apenas o número aproximado de mensagens
            )
            approximate_number_of_messages = response.get("Attributes", {}).get(
                "ApproximateNumberOfMessages", "N/A"
            )  # Obtém o número aproximado de mensagens ou N/A se não existir
            print(
                f"Número aproximado de mensagens na fila: {approximate_number_of_messages}"
            )
            if int(approximate_number_of_messages) > 0:  # Verifica se há mensagens
                return True
            return False
        except Exception as e:
            # Imprime erro caso falhe ao verificar mensagens
            print(f"Erro ao verificar mensagens na fila: {e}")

    def delete_message_from_queue(self, queue_name: str, receipt_handle: str):
        # Método para deletar uma mensagem específica da fila usando o receipt_handle
        try:
            self.sqs.delete_message(
                QueueUrl=self.get_queue_url(queue_name), ReceiptHandle=receipt_handle
            )
            print("Mensagem deletada com sucesso.")  # Confirmação de exclusão
        except Exception as e:
            # Imprime erro caso falhe ao deletar
            print(f"Erro ao deletar mensagem da fila: {e}")

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
