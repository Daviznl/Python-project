import pandas as pd
import pymysql
import os
import sys
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
print("Conexão com o banco de dados estabelecida")
# Cursor para executar comandos SQL
cursor = conn.cursor()

# Caminho da pasta onde estão as planilhas
pasta = os.path.join(os.getenv('PATH_PLANILHAS'))

arquivos = []
# Lista os arquivos da pasta
planilhas = [f for f in os.listdir(pasta) if f.endswith('.xlsx')]
print(f"Encontradas {len(planilhas)} planilhas")

dfs = []
# Lê o nome das planilhas e busca no SQL se já foram processadas
for arquivo in planilhas:
    nome_arquivo = os.path.basename(arquivo)
    cursor.execute("SELECT 1 FROM arquivos_processados WHERE nome_arquivo = %s", (nome_arquivo,)) # Verifica se o arquivo já foi processado
    if cursor.fetchone(): # Se o arquivo já foi processado, pula
        print(f"Arquivo {nome_arquivo} já processado")
        continue

    df = pd.read_excel(os.path.join(pasta, arquivo))
    df = df.where(pd.notnull(df), 0)# Substitui valores nulos por 0
    dfs.append((df))# Adiciona o dataframe à lista
    arquivos.append(nome_arquivo)# Adiciona o nome do arquivo à lista

df_total = pd.concat(dfs, ignore_index=True)# Concatena todos os dataframes

# Itera sobre as linhas do dataframe
for index, row in df_total.iterrows():
    sku = row['SKU (Armazém)']
    data_venda = row['Horário Programado']
    custo = row['Custo do Produto']
    vendido = row['Qtd. do Produto']
    valor = row['Valor do Pedido']
    id_venda = row['Nº de Pedido']
    plataforma = row['Plataformas']
    loja = row['Nome da Loja no UpSeller']

    # Busca as informações do SKU na tabela sku no MySQL
    cursor.execute("SELECT modelo, cor_pr, cor_se, tamanho, enfeite FROM sku WHERE sku = %s", (sku,))
    sku_info = cursor.fetchone()

    if sku_info:
        modelo, cor_pr, cor_se, tamanho, enfeite = sku_info

        # Insere a venda no banco de dados
        cursor.execute("""
            INSERT INTO vendas (sku, data_venda, custo, quantidade, preco, id_venda, plataforma, loja, modelo, cor_pr, cor_se, tamanho, enfeite)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (sku, data_venda, custo, vendido, valor, id_venda, plataforma, loja, modelo, cor_pr, cor_se, tamanho, enfeite))
        
        # Atualiza o estoque
        if modelo in ["BIRKEN PALA", "BIRKEN CRUZADA", "SLIP ON"]:# Atualização condicional (Quando o modelo usa cor_pr e cor_se)
            cursor.execute("UPDATE estoque SET quantidade = quantidade - %s WHERE modelo = %s AND cor_pr = %s AND cor_se = %s AND tamanho = %s",
            (vendido, modelo, cor_pr, cor_se, tamanho))
        elif modelo in["CALCE FACIL"]:# Atualização condicional (Quando o modelo usa SKU como cor_pr)
            cursor.execute("UPDATE estoque SET quantidade = quantidade -%s WHERE modelo = %s AND cor_pr = %s AND tamanho = %s",
            (vendido, modelo, sku, tamanho))
        else:# Atualização padrão
            cursor.execute("UPDATE estoque  SET quantidade = quantidade - %s WHERE modelo = %s AND cor_pr = %s AND tamanho = %s",
            (vendido, modelo, cor_pr, tamanho))
        
    else:# Se o SKU não for encontrado, encerra o programa
        print(f"SKU {sku} não encontrado")
        sys.exit()
        
for nome_arquivo in arquivos:# Adiciona o nome do arquivo à tabela arquivos_processados no MySQL
    cursor.execute("INSERT INTO arquivos_processados (nome_arquivo) VALUES (%s)", (nome_arquivo,))


print(f"Arquivo {[nome_arquivo]} adicionado à lista de arquivos processados")
conn.commit()

cursor.close()
conn.close()

print("Conexão com o banco de dados encerrada")

print("Processamento finalizado")
