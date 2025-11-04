import pandas as pd

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
