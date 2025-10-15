# Projeto: Extração de dados PDF com AWS e Postgres

## Objetivo
Este projeto automatiza o processamento de arquivos PDF enviados para um bucket S3 da AWS, extrai informações relevantes e as armazena em um banco de dados PostgreSQL. Além disso, utiliza filas SQS para monitorar novos PDFs e gerenciar o fluxo de processamento de forma assíncrona.

---

## Arquitetura e Fluxo do Projeto

O fluxo principal do projeto é o seguinte:

1. **Fila SQS**:  
   - Mensagens com informações de novos PDFs no bucket S3 são enviadas para uma fila SQS.  
   - O script `PDFSQLlista` verifica se há mensagens na fila e processa cada PDF informado.

2. **Bucket S3**:  
   - Os arquivos PDF são armazenados em um bucket S3 da AWS.  
   - A classe `AWSS3` é responsável por baixar, enviar ou deletar arquivos do S3.

3. **Processamento de PDF**:  
   - O script `PDFTextExtract` faz o download do PDF, extrai o texto, aplica expressões regulares para capturar as operações relevantes e converte os dados em um DataFrame do Pandas.  

4. **Banco de Dados PostgreSQL**:  
   - O DataFrame gerado a partir do PDF é enviado para a tabela `corretora_pdf` no banco PostgreSQL.  
   - A classe `RDSPostgresSQLManager` gerencia a conexão com o banco, permitindo operações com psycopg2 ou SQLAlchemy.

5. **Serviço banco Postgres:**

**Supabase**
   



