from abc import abstractmethod


class Entity:
    @property
    @abstractmethod
    def urn(self) -> str:
        pass
