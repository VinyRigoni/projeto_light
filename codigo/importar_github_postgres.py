import os
import io
import requests
import pandas as pd
from sqlalchemy import create_engine, text

# --------------------------------------------------------
# 🔧 CONFIGURAÇÕES
# --------------------------------------------------------

# Pasta do GitHub (versão RAW da pasta)
GITHUB_REPO = "https://raw.githubusercontent.com/VinyRigoni/projeto_light/main/database_final/"

# Lista de arquivos CSV que você quer importar
ARQUIVOS = [
    "clientes.csv",
    "dim_localidade.csv",
    "perdas_energia.csv",
    "ocorrencias_tecnicas.csv",
    "medicoes_energia.csv"
]

# Nome do banco e schema
DATABASE = "database_light"
SCHEMA = "projeto_light"

# Usuário logado no Windows (dinâmico)
usuario_windows = os.getlogin()

# Conexão com o PostgreSQL
senha_postgres = "data%402025%40"
engine = create_engine(f"postgresql+psycopg2://{usuario_windows}:{senha_postgres}@localhost:5432/{DATABASE}")


# --------------------------------------------------------
# 🧱 FUNÇÃO: Cria o schema (se não existir)
# --------------------------------------------------------
def criar_schema():
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA} AUTHORIZATION CURRENT_USER;"))
    print(f"✅ Schema '{SCHEMA}' verificado/criado.")


# --------------------------------------------------------
# 🧱 FUNÇÃO: Cria tabelas (baseado nos arquivos conhecidos)
# --------------------------------------------------------
def criar_tabelas():
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.clientes (
        id_cliente SERIAL PRIMARY KEY,
        nome_cliente VARCHAR(255),
        cidade VARCHAR(100),
        estado VARCHAR(100),
        tipo_cliente VARCHAR(50),
        data_adesao DATE,
        id_cidade INT,
        id_estado INT,
        id_cidade_estado VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS {SCHEMA}.dim_localidade (
        id_localidade SERIAL PRIMARY KEY,
        cidade VARCHAR(100),
        estado VARCHAR(100),
        id_cidade INT,
        id_estado INT,
        id_cidade_estado VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS {SCHEMA}.perdas_energia (
        id_perda SERIAL PRIMARY KEY,
        data_perda DATE,
        estado VARCHAR(100),
        perda_tecnica_kwh NUMERIC(15,2),
        perda_nao_tecnica_kwh NUMERIC(15,2)
    );

    CREATE TABLE IF NOT EXISTS {SCHEMA}.ocorrencias_tecnicas (
        id_ocorrencia SERIAL PRIMARY KEY,
        data_ocorrencia DATE,
        cidade VARCHAR(100),
        estado VARCHAR(100),
        tipo_ocorrencia VARCHAR(100),
        tempo_reparo_h NUMERIC(10,2),
        id_cidade INT,
        id_estado INT,
        id_cidade_estado VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS {SCHEMA}.medicoes_energia (
        id_medicao SERIAL PRIMARY KEY,
        id_cliente INT,
        data_medicao DATE,
        consumo_kwh NUMERIC(15,2),
        tipo_medicao VARCHAR(50),
        FOREIGN KEY (id_cliente) REFERENCES {SCHEMA}.clientes(id_cliente)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    print("✅ Tabelas criadas/verificadas.")


# --------------------------------------------------------
# 📥 FUNÇÃO: Importa CSVs do GitHub
# --------------------------------------------------------
def importar_csvs():
    for arquivo in ARQUIVOS:
        url = GITHUB_REPO + arquivo
        print(f"🔄 Baixando {arquivo} ...")
        try:
            response = requests.get(url)
            response.raise_for_status()
            df = pd.read_csv(io.StringIO(response.text), encoding="utf-8", sep=",")
            tabela = os.path.splitext(arquivo)[0]  # nome da tabela = nome do arquivo sem .csv
            df.to_sql(tabela, engine, schema=SCHEMA, if_exists="append", index=False)
            print(f"✅ {arquivo} importado com sucesso para {SCHEMA}.{tabela}")
        except Exception as e:
            print(f"❌ Erro ao importar {arquivo}: {e}")


# --------------------------------------------------------
# 🚀 EXECUÇÃO
# --------------------------------------------------------
if __name__ == "__main__":
    criar_schema()
    criar_tabelas()
    importar_csvs()
    print("\n🎯 Processo concluído!")
