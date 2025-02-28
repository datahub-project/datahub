class DatabaseParams:
    dataset_part_0: str
    dataset_part_1: str
    dataset_part_2: str

    def __init__(
        self,
        dataset_part_0: str,
        dataset_part_1: str,
        dataset_part_2: str,
    ):
        self.dataset_part_0 = dataset_part_0
        self.dataset_part_1 = dataset_part_1
        self.dataset_part_2 = dataset_part_2

    @property
    def catalog(self) -> str:
        return self.dataset_part_0

    @property
    def database(self) -> str:
        return self.dataset_part_0

    @property
    def project(self) -> str:
        return self.dataset_part_0

    @property
    def schema(self) -> str:
        return self.dataset_part_1

    @property
    def dataset(self) -> str:
        return self.dataset_part_1

    @property
    def table(self) -> str:
        return self.dataset_part_2


class SourceOperationParams:
    start_time_millis: int
    end_time_millis: int
    database_params: DatabaseParams

    def __init__(
        self,
        start_time_millis: int,
        end_time_millis: int,
        database_params: DatabaseParams,
    ):
        self.start_time_millis = start_time_millis
        self.end_time_millis = end_time_millis
        self.database_params = database_params

    @property
    def catalog(self) -> str:
        return self.database_params.catalog

    @property
    def database(self) -> str:
        return self.database_params.database

    @property
    def project(self) -> str:
        return self.database_params.project

    @property
    def schema(self) -> str:
        return self.database_params.schema

    @property
    def dataset(self) -> str:
        return self.database_params.dataset

    @property
    def table(self) -> str:
        return self.database_params.table
