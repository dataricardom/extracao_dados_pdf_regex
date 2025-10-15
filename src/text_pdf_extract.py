import logging
import os
import re
import pandas as pd
import PyPDF2
from src.configs.tools.postgres import RDSPostgresSQLManager
from src.configs.tools.aws.s3 import AWSS3 

logging.basicConfig(level=logging.INFO)

class PDFTextExtract:
    def __init__(self, pdf_file_path):
        self.pdf_file_path = pdf_file_path
        self.extracted_text = ""
        self.aws = AWSS3()
        
    def start(self):
        
        try:
            self.download_file()
            t = self.extract_text()
            r= self.extract_operation(t)
            split = self.split_text_by_newline(r)
            df = self.convert_to_daframe(split)
            insert = self.send_to_db(df, "corretora_pdf")
            print(insert)
        except Exception as e:
            print("Erro")

    def download_file(self):
        bucket = os.getenv("AWS_BUCKET")
        if not os.path.exists("download"):
            os.mkdir("download", exist_ok=True)
        return self.aws.download_file_from_s3(bucket,self.pdf_file_path,f"download/{self.pdf_file_path}")
    
    def extract_text(self):
        with open(f"download/{self.pdf_file_path}", "rb") as file:
            pdf_reader = PyPDF2.PdfReader(file)

            extracted_text = ""

            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                extracted_text += page.extract_text()
            
        return extracted_text
    
    def extract_operation(self,text):
        padrao = r"(C/V.*?)(?=\nPosição Ajuste)"
        result = re.search(padrao, text, re.DOTALL)

        if result:
            return result.group(1)
        else:
            print("Não foi encontrado")

    def split_text_by_newline(self, text):
        if text:
            return text.split("\n")
        else:
            return []
    
    def convert_to_daframe(self, vetor):
        header = vetor[0].split()
        dados = (
            linha.split() for linha in vetor[1:] if linha
        )
        df = pd.DataFrame(dados, columns=header)

        return df
    
    def send_to_db(self, df, table_name):
        try:
            connection = RDSPostgresSQLManager().alchemy()
            df.to_sql(table_name, connection, if_exists="append", index=False)
            logging.info(f"Insert na {table_name} realizado no banco com sucesso!")
            os.remove(f"download/{self.pdf_file_path}")
        except Exception as e:
            raise print({e})

        
if __name__ == "__main__":
    PDFTextExtract("corretora_jornada_de_dados (2).pdf").start()