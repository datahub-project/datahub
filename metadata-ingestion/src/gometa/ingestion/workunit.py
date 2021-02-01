from abc import abstractmethod

class WorkUnit:
"""An instance of a unit of work"""
    @abstractmethod
    def set_id(self, id: str) -> WorkUnit:
    """Implementations must store the identifier of this workunit"""
        pass

    @abstractmethod
    def get_id(self) -> str:
    """Return the identifier for this workunit"""
        pass

    @abstractmethod
    def get_metadata(self) -> dict:
        pass



class SimpleWorkUnit(WorkUnit):
    
    def __init__(self):
        self.workunit_state = {}

    def get_metadata(self):
        return self.workunit_state


