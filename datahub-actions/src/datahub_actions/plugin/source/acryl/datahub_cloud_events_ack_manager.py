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
        if not processed:
            # Batching actions defer the ack and send it once the batch is written.
            logger.debug(f"Event not acked yet: {meta}")
            return
        # An ack covers every earlier event, like a Kafka offset commit. Actions
        # that batch (Action.act() returns False) ack only the last event of
        # each written batch.
        acked = (meta["batch_id"], meta["msg_id"])
        for key in [k for k in self.acks if k <= acked]:
            del self.acks[key]

    def outstanding_acks(self) -> int:
        return len(self.acks)
