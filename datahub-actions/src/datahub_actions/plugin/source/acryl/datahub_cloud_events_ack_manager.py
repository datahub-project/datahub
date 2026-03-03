import logging
from typing import Any, Dict, Tuple

logger = logging.getLogger(__name__)


class AckManager:
    """
    An internal Ack manager which hands out fake message ids but ensures we
    track all acks and processing
    """

    def __init__(self) -> None:
        self.batch_id = 0
        self.msg_id = 0
        self.acks: Dict[Tuple[int, int], bool] = {}

    def new_batch(self) -> None:
        self.batch_id += 1
        self.msg_id = 0

    def get_meta(self, event: Any) -> Dict[str, Any]:
        self.msg_id += 1
        self.acks[(self.batch_id, self.msg_id)] = event
        return {"batch_id": self.batch_id, "msg_id": self.msg_id}

    def ack(self, meta: dict, processed: bool) -> None:
        batch_id, msg_id = (meta["batch_id"], meta["msg_id"])
        if processed:
            self.acks.pop((batch_id, msg_id))
        else:
            logger.warning(f"Whoops - we didn't process {meta}")

    def outstanding_acks(self) -> int:
        return len(self.acks)
