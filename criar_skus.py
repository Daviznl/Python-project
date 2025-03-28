import pymysql
import pandas as pd
import os
from dotenv import load_dotenv # type: ignore
load_dotenv()
# Conexão com o banco de dados
conn = pymysql.Connect(
    host=os.getenv('DB_HOST'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASS'),
    database=os.getenv('DB_NAME'),
    port = int(os.getenv('DB_PORT'))
)
if conn:
    print("Conexão com o banco de dados estabelecida")

cursor = conn.cursor()


# Carrega o arquivo Excel
df = pd.read_excel(os.getenv('PATH_SKU'))



query = "INSERT INTO sku(sku, modelo, cor_pr, cor_se, tamanho, enfeite ) VALUES (%s, %s, %s, %s, %s, %s)"

for _, row in df.iterrows():
    cursor.execute(query,(
    row['SKU'],
    row['MODELO'],
    row['COR PRIMARIA'],
    row['COR SECUNDARIA'],
    row['TAMANHO'],
    row['ENFEITE']
    ))
    print(f"Registro inserido: {row['SKU']} - {row['MODELO']}")

conn.commit()
print("Registro inserido com sucesso")
cursor.close()
conn.close()
print("Conexão com o banco de dados encerrada")
