from pathlib import Path
import re
import unicodedata
import pandas as pd
from datetime import datetime
from pathlib import Path

def registrar_log(mensagem: str, log_path: Path):
    """Grava logs de execução (data, hora e mensagem)."""
    log_path.parent.mkdir(parents=True, exist_ok=True)

    with open(log_path, 'a', encoding='utf-8') as log_file:
        agora = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        log_file.write(f"[{agora}] {mensagem}\n")

def limpar_nulos(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(how='all')

def padronizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    return df

def remover_duplicatas(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates()

def padronizar_datas(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        try:
            df[col] = pd.to_datetime(df[col], errors='ignore', dayfirst=True)
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime("%d/%m/%Y")
        except Exception:
            pass
    return df

def aplicar_tratativas(df: pd.DataFrame) -> pd.DataFrame:
    df = limpar_nulos(df)
    df = remover_duplicatas(df)
    df = padronizar_colunas(df)
    df = padronizar_datas(df)
    return df

def limpar_nome(texto: str) -> str:
    if not isinstance(texto, str):
        return texto

    texto = texto.strip()

    # Remove pronomes de tratamento comuns
    texto = re.sub(
        r'\b(sr\.?|sra\.?|srta\.?|senhor(a)?|dr\.?|dra\.?|prof\.?|professor(a)?|'
        r'eng\.?|engenheiro(a)?|rev\.?|reverendo(a)?|mr\.?|mrs\.?|miss|ms\.?)\b',
        '',
        texto,
        flags=re.IGNORECASE
    )

    # Remove caracteres especiais do início e fim (pontos, vírgulas, hífens, etc.)
    texto = re.sub(r'^[\W_]+|[\W_]+$', '', texto)

    # Remove múltiplos espaços e pontuação duplicada
    texto = re.sub(r'\s+', ' ', texto)
    texto = re.sub(r'[.,;:!]+', '', texto)

    # Remove acentos (opcional – deixe comentado se quiser manter)
    texto = ''.join(
        c for c in unicodedata.normalize('NFKD', texto)
        if not unicodedata.combining(c)
    )

    return texto

from pathlib import Path
import pandas as pd
import re
import unicodedata
import hashlib

def limpar_nome(nome: str) -> str:
    """Remove pronomes de tratamento, acentos e caracteres especiais."""
    if pd.isna(nome):
        return ""
    nome = str(nome).lower().strip()
    nome = re.sub(r"\b(sr|sra|srta|dr|dra|prof|profª)\.?\b", "", nome)
    nome = ''.join(c for c in unicodedata.normalize('NFD', nome) if unicodedata.category(c) != 'Mn')
    nome = re.sub(r"[^a-zA-Z0-9\s]", "", nome)
    return nome.strip().title()

def gerar_ids_dim_localidade(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gera IDs numéricos:
      - id_cidade: 4 dígitos (0001, 0002, ...)
      - id_estado: 2 dígitos (01, 02, ...)
      - id_cidade_estado: concatenação literal id_cidade + id_estado (6 dígitos)
    Retorna o dataframe com as colunas id_cidade, id_estado e id_cidade_estado adicionadas.
    """
    # Normaliza colunas
    df["cidade"] = df["cidade"].astype(str).str.strip().str.title()
    df["estado"] = df["estado"].astype(str).str.strip().str.upper()

    # --- Gera dimensão de cidades
    cidades_unicas = pd.DataFrame(df["cidade"].dropna().unique(), columns=["cidade"])
    cidades_unicas = cidades_unicas.sort_values(by="cidade", ignore_index=True)
    cidades_unicas["id_cidade"] = [f"{i:04d}" for i in range(1, len(cidades_unicas) + 1)]  # ← começa em 0001

    # --- Gera dimensão de estados
    estados_unicos = pd.DataFrame(df["estado"].dropna().unique(), columns=["estado"])
    estados_unicos = estados_unicos.sort_values(by="estado", ignore_index=True)
    estados_unicos["id_estado"] = [f"{i:02d}" for i in range(1, len(estados_unicos) + 1)]  # ← começa em 01

    # --- Junta IDs ao dataframe principal
    df = df.merge(cidades_unicas, on="cidade", how="left")
    df = df.merge(estados_unicos, on="estado", how="left")

    # --- Concatena para formar id_cidade_estado (6 dígitos)
    df["id_cidade_estado"] = df["id_cidade"].astype(str) + df["id_estado"].astype(str)

    return df, cidades_unicas, estados_unicos

def converter_csv_para_xlsx(caminho_csv: Path, output_dir: Path, verbose: bool = False) -> Path:
    """
    Lê um arquivo CSV, remove duplicatas, aplica regras específicas e salva como XLSX.
    Regras:
    - Padroniza datas (formato dd/mm/yyyy)
    - Remove duplicatas
    - Se for clientes.csv → limpa nome_cliente e gera IDs de localidade
    - ocorrencias_tecnicas.csv não gera IDs
    """

    # Detecta delimitador e encoding
    try:
        df = pd.read_csv(caminho_csv, sep=None, engine="python", encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(caminho_csv, sep=None, engine="python", encoding="latin1")

    # Padroniza colunas
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    df = df.drop_duplicates()

    nome_arquivo = caminho_csv.name.lower()

    # 🔹 Limpa nome_cliente e gera IDs apenas se for clientes.csv
    if nome_arquivo == "clientes.csv":
        if {"nome_cliente", "cidade"}.issubset(df.columns):
            df["nome_cliente"] = df["nome_cliente"].apply(limpar_nome)

        # 🔹 Gera IDs para cidade e estado
        if {"cidade", "estado"}.issubset(df.columns):
            df, cidades_unicas, estados_unicos = gerar_ids_dim_localidade(df)

            # Cria a dimensão final consolidada
            dim_localidade = (
                df[["cidade", "estado", "id_cidade", "id_estado", "id_cidade_estado"]]
                .drop_duplicates()
                .sort_values(by=["estado", "cidade"])
            )

            # Garante diretório
            output_dir.mkdir(parents=True, exist_ok=True)

            # Salva a dimensão
            caminho_dim = output_dir / "dim_localidade.xlsx"
            dim_localidade.to_excel(caminho_dim, index=False)

            if verbose:
                print(f"📘 Dimensão 'dim_localidade.xlsx' criada em: {caminho_dim}")
        elif verbose:
            print("⚠️ Colunas 'cidade' e/ou 'estado' não encontradas em clientes.csv para gerar IDs.")

    elif verbose and nome_arquivo == "ocorrencias_tecnicas.csv":
        print("ℹ️ Nenhum ID será gerado para ocorrencias_tecnicas.csv.")

    # 🔹 Padroniza datas
    for coluna in df.columns:
        if re.search(r"data|date", coluna, re.IGNORECASE):
            df[coluna] = pd.to_datetime(df[coluna], errors="coerce").dt.strftime("%d/%m/%Y")

    # 🔹 Garante diretório de saída
    output_dir.mkdir(parents=True, exist_ok=True)

    # 🔹 Salva arquivo tratado
    caminho_saida = output_dir / f"{caminho_csv.stem}_tratado.xlsx"
    df.to_excel(caminho_saida, index=False)

    if verbose:
        print(f"✅ Arquivo convertido com sucesso: {caminho_saida}")

    return caminho_saida
