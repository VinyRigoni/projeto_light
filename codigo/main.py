from pathlib import Path
from utils import registrar_log, converter_csv_para_xlsx
import time

# Caminhos base
base_path = Path(__file__).parent
database_dir = base_path.parent / "database"
output_dir = base_path.parent / "database_final"
log_file = base_path.parent / "logs" / "execucao.log"

# Lista todos os CSVs dentro da pasta database
csv_files = list(database_dir.glob("*.csv"))

inicio = time.time()
registrar_log("Iniciando processo de transformação...", log_file)

for csv_path in csv_files:
    try:
        xlsx_path = converter_csv_para_xlsx(csv_path, output_dir)
        registrar_log(f"✅ {csv_path.name} convertido com sucesso → {xlsx_path.name}", log_file)
    except Exception as e:
        registrar_log(f"Erro ao processar {csv_path.name}: {str(e)}", log_file)

fim = time.time()
duracao = round(fim - inicio, 2)

registrar_log(f"Processo concluído em {duracao} segundos.", log_file)
print("Transformação concluída com sucesso!")
