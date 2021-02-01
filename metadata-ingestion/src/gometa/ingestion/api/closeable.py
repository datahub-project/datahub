from abc import abstractmethod, ABCMeta

class Closeable(metaclass=ABCMeta):
    @abstractmethod
    def close(self):
        pass


