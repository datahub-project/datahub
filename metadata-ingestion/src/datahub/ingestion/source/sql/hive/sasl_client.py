import struct
from typing import Any, List, Optional, Tuple, Union

from puresasl.client import SASLClient, SASLError

# thrift_sasl frames every SASL-wrapped message with a 4-byte big-endian length.
_SASL_LENGTH_HEADER = struct.Struct(">I")


class PureSASLClient(SASLClient):
    """pure-sasl client exposing the python-sasl interface that thrift_sasl calls.

    impyla's ``impala.sasl_compat.PureSASLClient`` (the class this replaces, and which
    ``thrift_sasl`` expects) swaps ``encode``/``decode`` and omits the length header.
    Both only matter once a QOP of ``auth-int`` or ``auth-conf`` is negotiated, which
    is why plain Kerberos (``auth``) works with it and integrity/confidentiality
    protected HiveServer2 / Hive Metastore endpoints fail.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.error: Optional[str] = None
        super().__init__(*args, **kwargs)

    def start(
        self, mechanism: Union[str, List[str]]
    ) -> Tuple[bool, Optional[str], Optional[bytes]]:
        self.error = None
        try:
            self.choose_mechanism(
                mechanism if isinstance(mechanism, list) else [mechanism]
            )
            return True, self.mechanism, self.process()
        except SASLError as e:
            self.error = str(e)
            return False, None, None

    def encode(self, outgoing: bytes) -> Tuple[bool, Optional[bytes]]:
        self.error = None
        try:
            wrapped = self.wrap(outgoing)
            return True, _SASL_LENGTH_HEADER.pack(len(wrapped)) + wrapped
        except Exception as e:
            self.error = str(e)
            return False, None

    def decode(self, incoming: bytes) -> Tuple[bool, Optional[bytes]]:
        self.error = None
        try:
            # thrift_sasl hands over the frame including its length header.
            return True, self.unwrap(incoming[_SASL_LENGTH_HEADER.size :])
        except Exception as e:
            self.error = str(e)
            return False, None

    def step(self, challenge: Optional[bytes] = None) -> Tuple[bool, Optional[bytes]]:
        self.error = None
        try:
            return True, self.process(challenge)
        except Exception as e:
            self.error = str(e)
            return False, None

    def getError(self) -> Optional[str]:
        return self.error
