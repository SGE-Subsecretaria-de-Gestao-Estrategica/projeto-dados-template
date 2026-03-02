from __future__ import annotations
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pathlib import Path
from typing import Literal, Union

FileExt     = Literal["pickle", "pkl", "parquet", "csv"]
AreaType    = Literal["raw", "interim", "processed", "external", "final"]

def load_to_dataframe(
        file_path: Union[str, Path], 
        sheet_name: str = None, 
        dtype: bool = False
) -> pd.DataFrame:
    """
    Carrega um arquivo em um DataFrame com base na extensão.

    :param file_path: Caminho do arquivo de entrada.
    :type file_path: str | pathlib.Path
    :param sheet_name: Nome da planilha para arquivos Excel. Se ``None``, usa a planilha padrão.
    :type sheet_name: str | None
    :param dtype: Se ``True``, tenta ler colunas como string quando suportado.
    :type dtype: bool
    :returns: DataFrame carregado.
    :rtype: pandas.DataFrame
    :raises ValueError: Se a extensão do arquivo não for suportada.
    """
    file_path = Path(file_path)
    ext = file_path.suffix.lower()

    if ext in [".csv"]:
        if dtype is False:
            return pd.read_csv(file_path)
        else:
            return pd.read_csv(file_path, sep=';', dtype=str)

    elif ext in [".xlsx", ".xls"]:
        if sheet_name is None:
            return pd.read_excel(file_path, dtype=str)
        else:
            return pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)

    elif ext in [".parquet"]:
        if dtype is False:
            return pd.read_parquet(file_path, engine="pyarrow")
        table = pq.read_table(file_path)
        table = table.cast(pa.schema([(name, pa.string()) for name in table.schema.names]))
        return table.to_pandas()

    elif ext in [".json"]:
        return pd.read_json(file_path)

    elif ext in [".pkl", ".pickle"]:
        return pd.read_pickle(file_path)

    elif ext in [".txt"]:
        # Assume arquivo delimitado por ; ou ,
        try:
            return pd.read_csv(file_path, sep=";", dtype=str)
        except Exception:
            return pd.read_csv(file_path, sep=",", dtype=str)
    
    else:
        raise ValueError(f"Formato de arquivo não suportado: {ext}")
    
def save_dataframe(
    df: pd.DataFrame,
    output_folder: Path,
    filename: str,
    ext: FileExt = "parquet",
    inplace: bool = False,
    timestamp_fmt: str = "%Y-%m-%d_%H-%M",
    index: bool = False,
    parquet_engine: str = "pyarrow",
) -> Path:
    """
    Salva um DataFrame em ``pkl``, ``parquet`` ou ``csv`` com timestamp no nome.

    O arquivo gerado sempre inclui um sufixo de timestamp (até minutos) no padrão definido
    por ``timestamp_fmt``. Exemplo: ``minha_base__2026-02-02_09-30.parquet``.
       
    :param df: DataFrame a ser salvo.
    :type df: pandas.DataFrame
    :param output_folder: Diretório de saída.
    :type output_folder: pathlib.Path
    :param filename: Nome base do arquivo, sem extensão.
    :type filename: str
    :param ext: Formato do arquivo (``pickle``, ``pkl``, ``parquet`` ou ``csv``).
    :type ext: str
    :param inplace: Se ``True``, remove arquivos anteriores com o mesmo nome base e extensão.
    :type inplace: bool
    :param timestamp_fmt: Formato do timestamp usado no sufixo do nome.
    :type timestamp_fmt: str
    :param index: Se ``True``, salva o índice do DataFrame.
    :type index: bool
    :param parquet_engine: Engine de escrita para Parquet.
    :type parquet_engine: str
    :returns: Caminho completo do arquivo salvo.
    :rtype: pathlib.Path
    :raises ValueError: Se o formato informado em ``ext`` não for suportado.
    """
    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    ext_norm = ext.lower().lstrip(".")
    if ext_norm == "pickle":
        ext_norm = "pkl"

    if ext_norm not in {"pkl", "parquet", "csv"}:
        raise ValueError(
            f"Extensão/formato não suportado: {ext!r}. Use: 'pickle'/'pkl', 'parquet' ou 'csv'."
        )

    ts = datetime.now().strftime(timestamp_fmt)
    out_path = output_folder / f"{filename}__{ts}.{ext_norm}"

    if inplace:
        for p in output_folder.glob(f"*.{ext_norm}"):
            base = p.stem.split("__", 1)[0]
            if base == filename:
                p.unlink(missing_ok=True)

    if ext_norm == "pkl":
        df.to_pickle(out_path)

    elif ext_norm == "parquet":
        df.to_parquet(out_path, engine=parquet_engine, index=index)

    elif ext_norm == "csv":
        df_out = df.astype(str)
        df_out.to_csv(out_path, index=index, sep=";")

    return out_path
