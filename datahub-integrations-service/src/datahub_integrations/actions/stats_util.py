from enum import Enum


class Stage(Enum):
    LIVE = "live"
    BOOTSTRAP = "bootstrap"
    ROLLBACK = "rollback"
