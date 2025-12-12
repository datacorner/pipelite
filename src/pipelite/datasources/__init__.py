__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from .csvFileDS import csvFileDS
from .excelFileDS import excelFileDS
from .parquetFileDS import parquetFileDS
from .folderDS import folderDS

__all__ = [
    "csvFileDS",
    "excelFileDS",
    "parquetFileDS",
    "folderDS",
]

