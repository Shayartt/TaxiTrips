# First level imports
from dataclasses import dataclass
from abc import ABC, abstractmethod

import uuid

@dataclass
class Generator(ABC):
    """
    Abstract class for data generator
    
    Attributes
    ----------
    id : int
        The id of the instance.
    is_ready : bool
        The readiness of the data.

    """
    
    _id: int = None
    data: dict = None
    is_ready: bool = False
    
    def __post_init__(self):
        """
        Post initialization method.
        """
        self._id = uuid.uuid4().int
    
    def health_check(self) -> bool:
        """
        Health check method.
        
        :return: The health status of the instance.
        :rtype: bool
        """
        return self.is_ready
    
    @abstractmethod
    def generate(self) -> dict:
        """
        Abstract method to generate data.
        
        :return: The generated data.
        :rtype: dict
        """
        pass
    