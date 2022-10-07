import chardet

from pathlib import Path

def get_encoding(path:str)->str:
    """
    Получить кодировку файла, чтобы его можно было коректно прочитать
    """
    return chardet.detect(Path(path).read_bytes()).get('encoding')
