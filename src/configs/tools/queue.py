import json  # Para manipular dados em formato JSON
import os  # Para interagir com variáveis de ambiente
import re  # Para trabalhar com expressões regulares
import urllib.parse  # Para decodificar URLs e strings codificadas

from src.configs.tools.aws.sqs import AWSSQSManager  # Importa a classe personalizada para gerenciar SQS

class PDFSQLlista:
    def __init__(self):
        # Obtém o nome da fila SQS a partir de variável de ambiente
        self.queue = os.getenv("QUEUE_NAME")
        # Instancia o gerenciador de SQS
        self.sqs = AWSSQSManager()

    def check_messages(self):
        # Verifica se há mensagens na fila
        has_message = self.sqs.check_message_in_queue(self.queue)
        if has_message:
            # Recebe as mensagens da fila
            messages = self.sqs.receive_messages_from_queue(self.queue)

            # Itera sobre cada mensagem recebida
            for message in messages:
                receipt_handle = message["ReceiptHandle"]  # Obtém o identificador da mensagem (usado para deletar depois)
                
                # Converte o corpo da mensagem de JSON para dicionário Python
                json_body = json.loads(message["Body"])
                
                # Obtém a chave do objeto no S3 a partir da mensagem
                object_key = json_body["Records"][0]["s3"]["object"]["key"]
                
                # Decodifica a URL (substitui caracteres como %20 por espaços)
                object_key_unquote = urllib.parse.unquote(object_key)
                
                # Substitui sinais de '+' antes de parênteses por espaço (corrige nomes de arquivos)
                object_key_final = re.sub(r"\+(?=\()", " ", object_key_unquote)

                try:
                    # Aqui você processaria o PDF identificado
                    print("Processar PDF")

                except Exception as e:
                    # Captura erros durante o processamento do PDF
                    print("Erro ao processar PDF")

# Ponto de entrada do script
if __name__ == "__main__":
    # Instancia a classe e verifica mensagens na fila
    PDFSQLlista().check_messages()
