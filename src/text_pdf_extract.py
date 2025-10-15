import logging  # Para registrar informações e mensagens de log
import os  # Para interagir com o sistema de arquivos e variáveis de ambiente
import re  # Para trabalhar com expressões regulares
import pandas as pd  # Para manipulação de dados em formato de DataFrame
import PyPDF2  # Para leitura e extração de texto de arquivos PDF
from src.configs.tools.postgres import RDSPostgresSQLManager  # Classe personalizada para conexão com Postgres
from src.configs.tools.aws.s3 import AWSS3  # Classe personalizada para interação com AWS S3

# Configura o nível de log para INFO
logging.basicConfig(level=logging.INFO)

class PDFTextExtract:
    def __init__(self, pdf_file_path):
        # Inicializa o caminho do PDF
        self.pdf_file_path = pdf_file_path
        # Armazena o texto extraído do PDF
        self.extracted_text = ""
        # Instancia a classe AWSS3 para interagir com o S3
        self.aws = AWSS3()
        
    def start(self):
        # Método principal para orquestrar todas as etapas do processamento
        try:
            self.download_file()  # Faz o download do arquivo do S3
            t = self.extract_text()  # Extrai o texto do PDF
            r = self.extract_operation(t)  # Extrai a operação relevante usando regex
            split = self.split_text_by_newline(r)  # Divide o texto por quebras de linha
            df = self.convert_to_daframe(split)  # Converte a lista de linhas em um DataFrame
            insert = self.send_to_db(df, "corretora_pdf")  # Envia os dados para o banco
            print(insert)  # Imprime o resultado (pode ser None, pois o método não retorna valor)
        except Exception as e:
            # Captura qualquer erro durante o processamento
            print("Erro")  

    def download_file(self):
        # Método para baixar o PDF do S3
        bucket = os.getenv("AWS_BUCKET")  # Obtém o nome do bucket da variável de ambiente
        # Cria a pasta 'download' se não existir
        if not os.path.exists("download"):
            os.mkdir("download", exist_ok=True)
        # Baixa o arquivo do S3 para a pasta local
        return self.aws.download_file_from_s3(bucket, self.pdf_file_path, f"download/{self.pdf_file_path}")
    
    def extract_text(self):
        # Método para extrair texto do PDF
        with open(f"download/{self.pdf_file_path}", "rb") as file:  # Abre o arquivo PDF em modo binário
            pdf_reader = PyPDF2.PdfReader(file)  # Cria um leitor de PDF

            extracted_text = ""  # Inicializa a variável para armazenar o texto extraído

            # Itera por todas as páginas do PDF e extrai o texto
            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                extracted_text += page.extract_text()
            
        return extracted_text  # Retorna o texto extraído
    
    def extract_operation(self, text):
        # Método para extrair a operação relevante usando regex
        padrao = r"(C/V.*?)(?=\nPosição Ajuste)"  # Padrão regex para capturar entre "C/V" e "\nPosição Ajuste"
        result = re.search(padrao, text, re.DOTALL)  # Pesquisa usando regex, considerando múltiplas linhas

        if result:
            return result.group(1)  # Retorna o texto encontrado
        else:
            print("Não foi encontrado")  # Mensagem caso não encontre o padrão
    
    def split_text_by_newline(self, text):
        # Método para dividir o texto em linhas
        if text:
            return text.split("\n")  # Divide pelo caractere de nova linha
        else:
            return []  # Retorna lista vazia caso não haja texto
    
    def convert_to_daframe(self, vetor):
        # Método para converter lista de linhas em DataFrame
        header = vetor[0].split()  # Considera a primeira linha como cabeçalho
        dados = (
            linha.split() for linha in vetor[1:] if linha  # Divide cada linha em colunas
        )
        df = pd.DataFrame(dados, columns=header)  # Cria o DataFrame com cabeçalho definido

        return df  # Retorna o DataFrame
    
    def send_to_db(self, df, table_name):
        # Método para enviar o DataFrame para o banco Postgres
        try:
            connection = RDSPostgresSQLManager().alchemy()  # Cria a conexão com o banco
            # Insere os dados no banco, adicionando ao final da tabela
            df.to_sql(table_name, connection, if_exists="append", index=False)
            logging.info(f"Insert na {table_name} realizado no banco com sucesso!")
            os.remove(f"download/{self.pdf_file_path}")  # Remove o arquivo baixado após o processamento
        except Exception as e:
            # Captura erros na inserção
            raise print({e})  

# Ponto de entrada do script
if __name__ == "__main__":
    PDFTextExtract("corretora_jornada_de_dados (3).pdf").start()  # Instancia a classe e inicia o processamento
