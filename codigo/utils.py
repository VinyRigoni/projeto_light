from datetime import datetime
from pathlib import Path


def registrar_log(mensagem: str, log_path: Path):
    """Grava logs de execução (data, hora e mensagem)."""
    log_path.parent.mkdir(parents=True, exist_ok=True)

    with open(log_path, 'a', encoding='utf-8') as log_file:
        agora = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        log_file.write(f"[{agora}] {mensagem}\n")
