from pathlib import Path

# CAMINHO DOS DIRETÓRIOS
ROOT = Path(__file__).resolve().parents[2] # PASTA RAÍZ DO PROJETO

DATA_PATH               = ROOT / "data" 
RAW_DATA_PATH           = DATA_PATH / "raw" 
PROCESSED_DATA_PATH     = DATA_PATH / "processed" 
INTERIM_DATA_PATH       = DATA_PATH / "interim" 
EXTERNAL_DATA_PATH      = DATA_PATH / "external" 
FINAL_DATA_PATH         = DATA_PATH / "final" 