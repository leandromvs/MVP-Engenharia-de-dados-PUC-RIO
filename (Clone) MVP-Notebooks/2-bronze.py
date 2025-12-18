# Databricks notebook source
# Seleciona o catálogo e o schema bronze
spark.sql("USE CATALOG mvp")
spark.sql("USE SCHEMA bronze")

# COMMAND ----------

# Lê o CSV dos dados dos postos ANP usando tabulação como separador
df = spark.read.option("header", True).option("sep", "\t").csv("/Volumes/mvp/staging/arquivos-postos-anp/dados-postos-ANP.csv")
display(df.limit(10))

# COMMAND ----------

import re

# Lê o CSV do Volume Staging contendo os dados dos postos ANP
df_csv = (spark.read
    .option("header", True)
    .option("inferSchema", True)   
    .csv("/Volumes/mvp/staging/arquivos-postos-anp/dados-postos-ANP.csv")
)

# Renomeia a primeira coluna vazia para 'Num_reg_anp', reduz nomes grandes e remove caracteres não permitidos
def clean_col(col):
    col = col[:30] if len(col) > 30 else col
    col = re.sub(r'[^a-zA-Z0-9_]', '_', col)
    return col

new_columns = ['Num_reg_anp'] + [clean_col(col) for col in df_csv.columns[1:]]
df_csv = df_csv.toDF(*new_columns)

# COMMAND ----------

# Grava o DataFrame df_csv como uma tabela Delta chamada 'dados_postos_anp' no catálogo/schema atual, sobrescrevendo se já existir
df_csv.write.format("delta").mode("overwrite").saveAsTable("dados_postos_anp")

# Exibe uma amostra da tabela gravada
display(spark.table("dados_postos_anp").limit(10))

# COMMAND ----------

# Exibe a quantidade de colunas e linhas da tabela 'dados_postos_anp' no schema bronze
df_bronze = spark.table("dados_postos_anp")
num_colunas = len(df_bronze.columns)
num_linhas = df_bronze.count()
display({"Quantidade de colunas": num_colunas, "Quantidade de linhas": num_linhas})

# COMMAND ----------

# Exibe uma amostra dos dados gravados na tabela bronze
display(spark.table("dados_postos_anp").limit(10))

# COMMAND ----------

spark.sql("""
  COMMENT ON TABLE mvp.bronze.dados_postos_anp IS 
  'Tabela contendo informações dos postos de combustíveis cadastrados pela ANP, incluindo número de registro, localização, razão social, CNPJ e demais dados relevantes.'
""")

# COMMAND ----------

columns_comments = [
    ("Num_reg_anp", "Número de registro ANP do posto"),
    ("Data_Publica__o_DOU___Autoriza", "Data de publicação no DOU da autorização do posto"),
    ("C_digo_Instala__o_i_Simp", "Código de instalação do posto no sistema ANP"),
    ("Raz_o_Social", "Razão social do posto de combustíveis"),
    ("CNPJ", "CNPJ do posto de combustíveis"),
    ("Endere_o", "Endereço do posto"),
    ("COMPLEMENTO", "Complemento do endereço"),
    ("BAIRRO", "Bairro onde o posto está localizado"),
    ("CEP", "CEP do endereço do posto"),
    ("UF", "Unidade Federativa (estado) do posto"),
    ("MUNIC_PIO", "Município onde o posto está localizado"),
    ("Vincula__o_a_Distribuidor", "Distribuidor ao qual o posto está vinculado"),
    ("Data_de_Vincula__o_a_Distribui", "Data de vinculação do posto ao distribuidor"),
    ("Produto", "Produto comercializado pelo posto"),
    ("Tancagem__m__", "Capacidade de tancagem em metros cúbicos"),
    ("Qtde_de_Bico", "Quantidade de bicos de abastecimento"),
    ("LATITUDE", "Latitude geográfica do posto"),
    ("LONGITUDE", "Longitude geográfica do posto"),
    ("Delivery", "Indica se o posto possui serviço de delivery"),
    ("Data_Autoriza__o_Delivery", "Data de autorização do serviço de delivery"),
    ("N_mero_Despacho_Delivery", "Número do despacho de autorização do delivery"),
    ("Status_PMQC", "Status do Programa de Monitoramento da Qualidade de Combustíveis (PMQC)")
]

table = "mvp.bronze.dados_postos_anp"
for col, comment in columns_comments:
    spark.sql(f"COMMENT ON COLUMN {table}.{col} IS '{comment}'")
