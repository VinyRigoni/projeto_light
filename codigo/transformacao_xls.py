from pathlib import Path
import pandas as pd
import re
import unicodedata

def converter_csv_para_xlsx(caminho_csv: Path, output_dir: Path, verbose: bool = False) -> Path:
    """
    Lê um arquivo CSV, remove duplicatas, aplica regras específicas e salva como XLSX.
    Regras:
    - Padroniza datas (formato dd/mm/yyyy)
    - Remove duplicatas de linhas
    - Se for clientes.csv → concatena nome_cliente + cidade, remove repetições, pronomes de tratamento e caracteres especiais
    """

    # Detecta delimitador e encoding
    try:
        df = pd.read_csv(caminho_csv, sep=None, engine="python", encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(caminho_csv, sep=None, engine="python", encoding="latin1")

    # Padroniza nomes de colunas
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Remove duplicatas de linhas
    df = df.drop_duplicates()

    # 🔹 Regra especial para clientes.csv
    if caminho_csv.name.lower() == "clientes.csv":
        if {"nome_cliente", "cidade"}.issubset(df.columns):
            # Concatena nome_cliente + cidade
            df["nome_cliente"] = df["nome_cliente"].astype(str) + " " + df["cidade"].astype(str)
            df = df.drop(columns=["cidade"])

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

                # Remove palavras repetidas (independente da posição)
                palavras = texto.split()
                vistas = set()
                resultado = []
                for p in palavras:
                    plower = p.lower()
                    if plower not in vistas:
                        resultado.append(p)
                        vistas.add(plower)

                # Reconstrói e limpa novamente espaços extras
                texto_final = " ".join(resultado).strip()

                return texto_final

            # Aplica a limpeza
            df["nome_cliente"] = df["nome_cliente"].apply(limpar_nome)

        elif verbose:
            print("⚠️  Colunas 'nome_cliente' e/ou 'cidade' não encontradas em clientes.csv.")

    # 🔹 Converte colunas de data
    for coluna in df.columns:
        if re.search(r"data|date", coluna, re.IGNORECASE):
            df[coluna] = pd.to_datetime(df[coluna], errors="coerce").dt.strftime("%d/%m/%Y")

    # 🔹 Garante que o diretório de saída exista
    output_dir.mkdir(parents=True, exist_ok=True)

    # 🔹 Define o nome do arquivo de saída
    caminho_saida = output_dir / f"{caminho_csv.stem}_tratado.xlsx"

    # 🔹 Salva o Excel
    df.to_excel(caminho_saida, index=False)

    if verbose:
        print(f"✅ Arquivo convertido com sucesso: {caminho_saida}")

    return caminho_saida
