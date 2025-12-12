__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from .log import log
from .plReport import plReport
from .plReports import plReports


class cursorByField:
    """Use cursor results by accessing fields directly as attributes.
    
    Args:
        cursor: SQL cursor
        row: Result set row
    """
    def __init__(self, cursor, row):
        for (attr, val) in zip((d[0] for d in cursor.description), row):
            setattr(self, attr, val)

    def get(self, attribute):
        """Get attribute value safely.
        
        Args:
            attribute: Attribute name to retrieve
            
        Returns:
            Attribute value or None if not found
        """
        return getattr(self, attribute, None)


__all__ = [
    "log",
    "plReport",
    "plReports",
    "cursorByField",
]