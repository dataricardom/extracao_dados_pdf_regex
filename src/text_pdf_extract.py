import logging
import os
import re
import pandas as pd
import PyPDF2

from src.configs.tools.aws.s3 import AWSS3 

logging.basicConfig(level=logging.INFO)

class PDFTextExtract:
    def __init__(self, pdf_file_path):
        self.pdf_file_path = pdf_file_path
        self.extracted_tex = ""
        self.aws = AWSS3()

    def start(self):
        
        try:
            self.download_file()
            self.extract_text()
        
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


if __name__ == "__main__":
    PDFTextExtract("corretora_jornada_de_dados (1).pdf").start()