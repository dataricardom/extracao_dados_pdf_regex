import json
import os
import re
import urllib.parse

from src.configs.tools.aws.sqs import AWSSQSManager

class PDFSQLlista:
    def __init__(self):
        self.queue = os.getenv("QUEUE_NAME")
        self.sqs = AWSSQSManager()

    def check_messages(self):
        has_message = self.sqs.check_message_in_queue(self.queue)
        if has_message:
            messages = self.sqs.receive_messages_from_queue(self.queue)

            for message in messages:
                receipt_handle = message["ReceiptHandle"]
                json_body = json.loads(message["Body"])
                objetct_key = json_body["Records"][0]["s3"]["object"]["key"]
                objetct_key_unquote = urllib.parse.unquote(objetct_key)
                objetct_key_final = re.sub(r"\+(?=\()", " ", objetct_key_unquote)

                try:
                    print("Processar PDF")

                except Exception as e:
                    print("Erro ao processar PDF")

if __name__ == "__main__":
    PDFSQLlista().check_messages()