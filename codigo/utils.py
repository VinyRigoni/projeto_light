from pathlib import Path
import re
import unicodedata
import pandas as pd
from datetime import datetime


def registrar_log(mensagem: str, log_path: Path):
    """Grava logs de execução (data, hora e mensagem)."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, 'a', encoding='utf-8') as log_file:
        agora = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        log_file.write(f"[{agora}] {mensagem}\n")


def limpar_nome(nome: str) -> str:
    """Remove pronomes de tratamento, acentos e caracteres especiais."""
    if pd.isna(nome):
        return ""
    nome = str(nome).lower().strip()
    nome = re.sub(r"\b(sr|sra|srta|dr|dra|prof|profª)\.?\b", "", nome)
    nome = ''.join(c for c in unicodedata.normalize('NFD', nome) if unicodedata.category(c) != 'Mn')
    nome = re.sub(r"[^a-zA-Z0-9\s]", "", nome)
    return nome.strip().title()


def normalizar_cidade(texto: str) -> str:
    """Converte texto em MAIÚSCULO e remove acentos."""
    if pd.isna(texto):
        return ""
    texto = str(texto).strip()
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    return texto.upper()


def gerar_ids_dim_localidade(df: pd.DataFrame) -> pd.DataFrame:
    """Gera IDs de cidade/estado e retorna DataFrame com colunas id_cidade, id_estado, id_cidade_estado."""
    df["cidade"] = df["cidade"].astype(str).str.strip().apply(normalizar_cidade)
    df["estado"] = df["estado"].astype(str).str.strip().str.upper()

    # Dimensão cidades
    cidades_unicas = pd.DataFrame(df["cidade"].dropna().unique(), columns=["cidade"])
    cidades_unicas = cidades_unicas.sort_values(by="cidade", ignore_index=True)
    cidades_unicas["id_cidade"] = [f"{i:03d}" for i in range(1, len(cidades_unicas) + 1)]

    # Dimensão estados (sempre 2 dígitos)
    estados_unicos = pd.DataFrame(df["estado"].dropna().unique(), columns=["estado"])
    estados_unicos = estados_unicos.sort_values(by="estado", ignore_index=True)
    estados_unicos["id_estado"] = [f"{i:02d}" for i in range(1, len(estados_unicos) + 1)]

    # Merge
    df = df.merge(cidades_unicas, on="cidade", how="left")
    df = df.merge(estados_unicos, on="estado", how="left")

    # id_cidade_estado → 5 algarismos (cidade 3 + estado 2)
    df["id_cidade_estado"] = df["id_cidade"].astype(str) + df["id_estado"].astype(str)

    return df, cidades_unicas, estados_unicos


def atualizar_dim_localidade(caminho_dim: Path, novas_linhas: pd.DataFrame, verbose=False):
    """Atualiza dim_localidade.csv com novas cidades/estados que ainda não existam."""
    if caminho_dim.exists():
        dim_localidade = pd.read_csv(caminho_dim, encoding="utf-8")
    else:
        dim_localidade = pd.DataFrame(columns=["cidade", "estado", "id_cidade", "id_estado", "id_cidade_estado"])

    # Normaliza
    dim_localidade["cidade"] = dim_localidade["cidade"].apply(normalizar_cidade)
    dim_localidade["estado"] = dim_localidade["estado"].str.upper()
    novas_linhas["cidade"] = novas_linhas["cidade"].apply(normalizar_cidade)
    novas_linhas["estado"] = novas_linhas["estado"].str.upper()

    # Verifica novas combinações cidade/estado
    novas = novas_linhas.merge(dim_localidade, on=["cidade", "estado"], how="left", indicator=True)
    novas = novas.loc[novas["_merge"] == "left_only", ["cidade", "estado"]]

    if novas.empty:
        if verbose:
            print("✅ Nenhuma nova localidade encontrada para adicionar.")
        return dim_localidade

    # 🔹 Mantém o mesmo id_estado para estados já existentes
    estados_existentes = dim_localidade[["estado", "id_estado"]].drop_duplicates()
    novos_estados = novas[~novas["estado"].isin(estados_existentes["estado"])].drop_duplicates(subset=["estado"])
    prox_id_estado = (
        dim_localidade["id_estado"].astype(str).astype(float).max() if not dim_localidade.empty else 0
    )
    prox_id_estado = int(prox_id_estado) + 1 if not pd.isna(prox_id_estado) else 1

    if not novos_estados.empty:
        novos_estados["id_estado"] = [
            f"{i:02d}" for i in range(prox_id_estado, prox_id_estado + len(novos_estados))
        ]
        estados_atualizados = pd.concat([estados_existentes, novos_estados], ignore_index=True)
    else:
        estados_atualizados = estados_existentes

    # Gera novos IDs de cidade (3 dígitos)
    prox_id_cidade = (
        dim_localidade["id_cidade"].astype(str).astype(float).max() if not dim_localidade.empty else 0
    )
    prox_id_cidade = int(prox_id_cidade) + 1 if not pd.isna(prox_id_cidade) else 1

    novas = novas.merge(estados_atualizados, on="estado", how="left")
    novas["id_cidade"] = [f"{i:03d}" for i in range(prox_id_cidade, prox_id_cidade + len(novas))]

    # id_cidade_estado = cidade (3) + estado (2)
    novas["id_cidade_estado"] = novas["id_cidade"].astype(str) + novas["id_estado"].astype(str)

    # 🔹 Atualiza e salva dimensão
    dim_atualizada = pd.concat([dim_localidade, novas], ignore_index=True)
    dim_atualizada = dim_atualizada.drop_duplicates(subset=["cidade", "estado"])
    dim_atualizada = dim_atualizada.sort_values(by=["estado", "cidade"], ignore_index=True)
    dim_atualizada.to_csv(caminho_dim, encoding="utf-8", index=False)

    if verbose:
        print(f"🔄 Dimensão localidade atualizada: {len(novas)} novas entradas adicionadas.")

    return dim_atualizada


def converter_csv_para_xlsx(caminho_csv: Path, output_dir: Path, verbose: bool = False) -> Path:
    """Lê um CSV, trata dados e exporta o resultado e as dimensões como CSV UTF-8 delimitado por vírgula."""
    try:
        df = pd.read_csv(caminho_csv, sep=None, engine="python", encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(caminho_csv, sep=None, engine="python", encoding="latin1")

    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    df = df.drop_duplicates()
    nome_arquivo = caminho_csv.name.lower()

    if "cidade" in df.columns:
        df["cidade"] = df["cidade"].apply(normalizar_cidade)
    if "estado" in df.columns:
        df["estado"] = df["estado"].str.upper()

    caminho_dim = output_dir / "dim_localidade.csv"

    # CLIENTES.CSV → cria dimensão
    if nome_arquivo == "clientes.csv":
        if "nome_cliente" in df.columns:
            df["nome_cliente"] = df["nome_cliente"].apply(limpar_nome)

        if {"cidade", "estado"}.issubset(df.columns):
            df, _, _ = gerar_ids_dim_localidade(df)
            dim_localidade = (
                df[["cidade", "estado", "id_cidade", "id_estado", "id_cidade_estado"]]
                .drop_duplicates()
                .sort_values(by=["estado", "cidade"])
            )
            output_dir.mkdir(parents=True, exist_ok=True)
            dim_localidade.to_csv(caminho_dim, encoding="utf-8", index=False)

            if verbose:
                print(f"📘 Dimensão 'dim_localidade.csv' criada em: {caminho_dim}")
        elif verbose:
            print("⚠️ Colunas 'cidade' e/ou 'estado' não encontradas.")

    # OCORRENCIAS_TECNICAS.CSV → atualiza dimensão existente
    elif nome_arquivo == "ocorrencias_tecnicas.csv":
        if not caminho_dim.exists():
            if verbose:
                print("⚠️ Dimensão 'dim_localidade.csv' não encontrada. Execute primeiro com clientes.csv.")
        else:
            novas_localidades = df[["cidade", "estado"]].drop_duplicates()
            dim_localidade = atualizar_dim_localidade(caminho_dim, novas_localidades, verbose=verbose)
            df = df.merge(dim_localidade, on=["cidade", "estado"], how="left")

            if verbose:
                print("🔗 IDs aplicados ao arquivo e dimensão atualizada.")

    # 🔹 PERDAS_ENERGIA.CSV → acrescenta id_estado
    elif nome_arquivo == "perdas_energia.csv":
        if not caminho_dim.exists():
            if verbose:
                print("⚠️ Dimensão 'dim_localidade.csv' não encontrada. Execute primeiro com clientes.csv.")
        else:
            dim_localidade = pd.read_csv(caminho_dim, encoding="utf-8")
            if {"estado"}.issubset(df.columns):
                df["estado"] = df["estado"].str.upper()
                df = df.merge(
                    dim_localidade[["estado", "id_estado"]].drop_duplicates(),
                    on="estado",
                    how="left"
                )
                if verbose:
                    print("📎 Coluna 'id_estado' adicionada ao arquivo perdas_energia.csv.")

    # Padroniza datas
    for coluna in df.columns:
        if re.search(r"data|date", coluna, re.IGNORECASE):
            df[coluna] = pd.to_datetime(df[coluna], errors="coerce").dt.strftime("%d/%m/%Y")

    # Salva saída em CSV UTF-8 com vírgulas
    output_dir.mkdir(parents=True, exist_ok=True)
    caminho_saida = output_dir / f"{caminho_csv.stem}_tratado.csv"
    df.to_csv(caminho_saida, encoding="utf-8", index=False)

    if verbose:
        print(f"✅ Arquivo convertido com sucesso: {caminho_saida}")

    return caminho_saida
