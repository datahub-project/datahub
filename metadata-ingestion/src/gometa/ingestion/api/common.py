from dataclasses import dataclass
from typing import TypeVar, Generic, Optional

T = TypeVar('T')

@dataclass
class RecordEnvelope(Generic[T]):
    record: T 
    metadata: Optional[dict]
    
    
