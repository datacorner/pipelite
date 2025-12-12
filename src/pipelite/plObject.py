from __future__ import annotations

__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from pipelite.utils.log import log
    from pipelite.config.plConfig import plConfig


class plObject:
    """Base class for all pipelite objects.
    
    Provides common configuration and logging functionality
    for all objects in the pipeline framework.
    """
    
    def __init__(self, config: Optional[plConfig], log: Optional[log]):
        """Initialize the base object.
        
        Args:
            config: Configuration object
            log: Logger instance
        """
        self.log = log
        self.config = config    