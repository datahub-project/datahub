import logging
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Optional, Sequence

import requests

from datahub.ingestion.source.qlik_sense.config import Constant, QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    Item,
    Sheet,
    Space,
    UnreadItem,
)
from datahub.ingestion.source.qlik_sense.script_lineage import (
    DataConnectionInfo,
    infer_data_connection,
)
from datahub.ingestion.source.qlik_sense.websocket_connection import WebsocketConnection
from datahub.utilities.backpressure_aware_executor import BackpressureAwareExecutor

logger = logging.getLogger(__name__)


def log_read_error(message: str) -> None:
    logger.warning(message)
    _, e, _ = sys.exc_info()
    if isinstance(e, requests.exceptions.HTTPError):
        logger.warning(f"HTTP status-code = {e.response.status_code}")
    logger.debug(msg=message, exc_info=e)


@dataclass
class ReadErrors:
    """Errors of the Engine reads for one app, passed down to every read so a
    failure is tied to the app it belongs to."""

    messages: List[str] = field(default_factory=list)

    def record(self, message: str) -> None:
        log_read_error(message)
        self.messages.append(message)


class QlikBaseAPI(ABC):
    """Cloud and Enterprise differ in how spaces, items and data connections are
    listed, but read apps from the Engine the same way."""

    def __init__(self, config: QlikSourceConfig) -> None:
        self.config = config
        self.spaces: Dict[str, str] = {}
        self.users: Dict[str, str] = {}
        # Spaces and items that failed to load, so the source can report them.
        self.unread_items: List[UnreadItem] = []

    def _record_unread(self, name: str, message: str, e: Exception) -> None:
        log_read_error(message)
        self.unread_items.append(UnreadItem(name=name, cause=str(e)))

    @abstractmethod
    def get_spaces(self) -> List[Space]: ...

    @abstractmethod
    def get_user_name(self, user_id: str) -> Optional[str]: ...

    @abstractmethod
    def get_data_connections(self) -> Dict[str, DataConnectionInfo]: ...

    @abstractmethod
    def _list_items(self, space_id: str) -> Iterable[Dict]:
        """Raw items of a space, recording the space as unread when listing fails."""

    @abstractmethod
    def _load_item(self, item: Dict) -> Optional[Item]:
        """Reads one listed item, recording it as unread when that fails."""

    @abstractmethod
    def _open_engine(self, app_id: str) -> WebsocketConnection: ...

    @abstractmethod
    def _sheet_meta(self, meta: Dict, sheet_id: str) -> Optional[Dict]:
        """Sheet fields from the sheet layout's qMeta, None to leave the sheet out."""

    def get_items(self, space_id: str) -> Iterable[Item]:
        # REST and Engine reads are network-bound; the GIL is only held while
        # decoding replies.
        for future in BackpressureAwareExecutor.map(
            self._load_item,
            ((item,) for item in self._list_items(space_id)),
            max_workers=self.config.max_workers,
        ):
            try:
                item = future.result()
            except Exception as e:
                self._record_unread(
                    f"item of space {space_id}",
                    f"Unable to load item. Exception: {e}",
                    e,
                )
                continue
            if item:
                yield item

    @staticmethod
    def _infer_connection(
        name: str, connection: Dict, descriptor_keys: Sequence[str]
    ) -> DataConnectionInfo:
        descriptor = ";".join(str(connection.get(key) or "") for key in descriptor_keys)
        return infer_data_connection(name, descriptor)

    @staticmethod
    def _close_engine(websocket_connection: WebsocketConnection, app_id: str) -> None:
        try:
            websocket_connection.close_websocket()
        except Exception as e:
            logger.debug(f"Unable to close Engine session of app {app_id}: {e}")

    @staticmethod
    def _read_load_script(
        websocket_connection: WebsocketConnection,
        app_id: str,
        on_error: Callable[[str], None],
    ) -> Optional[str]:
        try:
            script = websocket_connection.get_script()
            logger.debug(f"App {app_id} load script has {len(script)} characters")
            return script
        except Exception as e:
            on_error(f"Unable to fetch load script of app {app_id}. Exception: {e}")
        return None

    def _get_sheet(
        self,
        websocket_connection: WebsocketConnection,
        sheet_id: str,
        errors: ReadErrors,
    ) -> Optional[Sheet]:
        try:
            websocket_connection.websocket_send_request(
                method="GetObject", params={"qId": sheet_id}
            )
            response = websocket_connection.websocket_send_request(method="GetLayout")
            sheet_dict = response[Constant.QLAYOUT]
            meta = self._sheet_meta(sheet_dict[Constant.QMETA], sheet_id)
            if meta is None:
                return None
            sheet = Sheet.model_validate(meta)
            if Constant.QCHILDLIST not in sheet_dict:
                logger.debug(f"Sheet {sheet.title} with id {sheet_id} has no charts")
                return sheet
            for i, chart_dict in enumerate(
                sheet_dict[Constant.QCHILDLIST][Constant.QITEMS]
            ):
                for chart in websocket_connection.get_charts(
                    chart_dict[Constant.QINFO][Constant.QID],
                    sheet_id,
                    on_error=errors.record,
                ):
                    if not chart.title:
                        chart.title = f"Object {i + 1} of Sheet '{sheet.title}'"
                    sheet.charts.append(chart)
            return sheet
        except Exception as e:
            errors.record(f"Unable to fetch sheet with id {sheet_id}. Exception: {e}")
        return None

    def _get_app_sheets(
        self,
        websocket_connection: WebsocketConnection,
        app_id: str,
        errors: ReadErrors,
    ) -> List[Sheet]:
        sheets: List[Sheet] = []
        try:
            response = websocket_connection.websocket_send_request(
                method="GetObjects",
                params={"qOptions": {"qTypes": ["sheet"]}},
            )
            for sheet_dict in response[Constant.QLIST]:
                with websocket_connection.restore_handle():
                    sheet = self._get_sheet(
                        websocket_connection,
                        sheet_dict[Constant.QINFO][Constant.QID],
                        errors,
                    )
                if sheet:
                    sheets.append(sheet)
        except Exception as e:
            errors.record(f"Unable to fetch sheets for app {app_id}. Exception: {e}")
        return sheets
