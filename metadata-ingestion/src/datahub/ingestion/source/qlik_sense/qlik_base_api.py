import logging
import sys
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from typing import (
    Callable,
    Deque,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import requests

from datahub.ingestion.source.qlik_sense.config import Constant, QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    DataModelTable,
    Item,
    ListedDataset,
    Sheet,
    Space,
    UnchangedApp,
    UnreadItem,
)
from datahub.ingestion.source.qlik_sense.script_lineage import (
    DataConnectionInfo,
    infer_data_connection,
)
from datahub.ingestion.source.qlik_sense.websocket_connection import WebsocketConnection
from datahub.utilities.backpressure_aware_executor import BackpressureAwareExecutor

logger = logging.getLogger(__name__)

# Called with an app id and fingerprint; True skips reading that app.
IsUnchanged = Callable[[str, str], bool]


# The unread item a failed data connection listing records.
UNREAD_DATA_CONNECTIONS = "data connections"


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
    # Batch reads that fell back to one request per object: slower, not partial.
    fallbacks: List[str] = field(default_factory=list)
    # A sheet, or the list of sheets, could not be read, so the app's sheets
    # are not all known.
    sheets_unread: bool = False

    def record(self, message: str) -> None:
        log_read_error(message)
        self.messages.append(message)

    def record_fallback(self, message: str) -> None:
        logger.info(message)
        self.fallbacks.append(message)


class QlikBaseAPI(ABC):
    """Cloud and Enterprise differ in how spaces, items and data connections are
    listed, but read apps from the Engine the same way."""

    def __init__(self, config: QlikSourceConfig) -> None:
        self.config = config
        self.spaces: Dict[str, str] = {}
        self.users: Dict[str, str] = {}
        # User ids whose lookup failed, with the cause, so each is tried once
        # and reported once.
        self.failed_user_lookups: Dict[str, str] = {}
        # Spaces and items that failed to load, so the source can report them.
        self.unread_items: List[UnreadItem] = []
        # Apps read again because checking whether they changed failed.
        self.change_check_failures: List[str] = []
        # Datasets whose lineage graph could not be read.
        self.lineage_read_failures: List[str] = []
        self.field_lineage_failures: List[str] = []
        # Apps opened without their data because opening them with it failed.
        self.opened_without_data: List[str] = []

    def _record_failed_user_lookup(self, user_id: str, e: Exception) -> None:
        log_read_error(f"Unable to fetch user with id {user_id}. Exception: {e}")
        self.failed_user_lookups[user_id] = str(e)

    # Why the datasets of every space could not be listed, if they couldn't.
    dataset_listing_error: Optional[str] = None

    def _record_unread(self, name: str, message: str, e: Exception) -> None:
        log_read_error(message)
        self.unread_items.append(UnreadItem(name=name, cause=str(e)))

    @abstractmethod
    def get_spaces(self) -> List[Space]: ...

    @abstractmethod
    def get_user_name(self, user_id: str) -> Optional[str]: ...

    @abstractmethod
    def get_data_connections(self) -> Dict[str, DataConnectionInfo]: ...

    def list_datasets(self) -> Optional[List[ListedDataset]]:
        """Datasets of every space, or None when they could not be listed."""
        # Enterprise on Windows has no Qlik datasets.
        return []

    @abstractmethod
    def _list_items(self, space_id: str) -> Iterable[Dict]:
        """Raw items of a space, recording the space as unread when listing fails."""

    @abstractmethod
    def _load_item(
        self, item: Dict, is_unchanged: Optional[IsUnchanged]
    ) -> Union[Item, UnchangedApp, None]:
        """Reads one listed item, recording it as unread when that fails."""

    @abstractmethod
    def _open_engine(self, app_id: str) -> WebsocketConnection: ...

    @abstractmethod
    def _sheet_meta(self, meta: Dict, sheet_id: str) -> Optional[Dict]:
        """Sheet fields from the sheet layout's qMeta, None to leave the sheet out."""

    def get_items(
        self, space_ids: Sequence[str], is_unchanged: Optional[IsUnchanged] = None
    ) -> Iterable[Union[Item, UnchangedApp]]:
        """Items of all the spaces, read by one pool of workers, so sites with
        many small spaces or streams are read in parallel too. Items are taken
        from the spaces in turn, so a large space does not hold back the rest."""
        # REST and Engine reads are network-bound; the GIL is only held while
        # decoding replies.
        for future in BackpressureAwareExecutor.map(
            self._load_item_or_record,
            (
                (space_id, item, is_unchanged)
                for space_id, item in self._items_in_turn(space_ids)
            ),
            max_workers=self.config.resolved_max_workers(),
        ):
            item = future.result()
            if item:
                yield item

    def _items_in_turn(self, space_ids: Sequence[str]) -> Iterator[Tuple[str, Dict]]:
        # Each space is listed only when its turn first comes, so a slow
        # listing does not hold back the items already listed.
        unlisted = iter(space_ids)
        listings: Deque[Tuple[str, Iterator[Dict]]] = deque()
        while True:
            space_id = next(unlisted, None)
            if space_id is not None:
                listings.appendleft((space_id, iter(self._list_items(space_id))))
            elif not listings:
                return
            space_id, listing = listings.popleft()
            item = next(listing, None)
            if item is not None:
                yield space_id, item
                listings.append((space_id, listing))

    def _load_item_or_record(
        self, space_id: str, item: Dict, is_unchanged: Optional[IsUnchanged]
    ) -> Union[Item, UnchangedApp, None]:
        try:
            return self._load_item(item, is_unchanged)
        except Exception as e:
            item_id = item.get(Constant.RESOURCEID) or item.get(Constant.ID)
            self._record_unread(
                f"item {item_id} of space {space_id}",
                f"Unable to load item {item_id}. Exception: {e}",
                e,
            )
        return None

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
    def _read_data_model(
        websocket_connection: WebsocketConnection, app_id: str, errors: "ReadErrors"
    ) -> List[DataModelTable]:
        try:
            return [
                DataModelTable.from_engine(table)
                for table in websocket_connection.get_data_model_tables()
                if not table.get("qIsSynthetic")
            ]
        except Exception as e:
            errors.record(
                f"Unable to read the data model of app {app_id}. Exception: {e}"
            )
            return []

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

    @staticmethod
    def _read_variables(
        websocket_connection: WebsocketConnection, app_id: str
    ) -> Optional[Dict[str, str]]:
        # Without the variables, charts that expand one link to every table of
        # the app, as before, so a failed read leaves the app complete; None
        # tells the source to keep lineage that depends on them.
        try:
            return websocket_connection.get_variables()
        except Exception as e:
            log_read_error(f"Unable to read the variables of app {app_id}: {e}")
        return None

    def _get_sheet(
        self,
        websocket_connection: WebsocketConnection,
        sheet_id: str,
        errors: ReadErrors,
        keep_private: bool,
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
            if not keep_private and meta.get(Constant.PUBLISHED) is False:
                # A user's own sheet in a published app, which only they see.
                logger.debug(f"Skipping private sheet {sheet_id}")
                return None
            sheet = Sheet.model_validate(meta)
            if Constant.QCHILDLIST not in sheet_dict:
                logger.debug(f"Sheet {sheet.title} with id {sheet_id} has no charts")
                return sheet
            properties = websocket_connection.get_object_properties(
                on_fallback=lambda message: errors.record_fallback(
                    f"Sheet {sheet_id}: {message}"
                )
            )
            # Only a chart that could not be read leaves the sheet's chart list
            # incomplete; a chart whose measures were not read is still listed.
            unread_charts: List[str] = []

            def on_chart_error(message: str) -> None:
                unread_charts.append(message)
                errors.record(message)

            for i, chart_dict in enumerate(
                sheet_dict[Constant.QCHILDLIST][Constant.QITEMS]
            ):
                for chart in websocket_connection.get_charts(
                    chart_dict[Constant.QINFO][Constant.QID],
                    sheet_id,
                    on_error=on_chart_error,
                    properties=properties,
                    on_measure_error=errors.record,
                ):
                    if not chart.title:
                        chart.title = f"Object {i + 1} of Sheet '{sheet.title}'"
                    sheet.charts.append(chart)
            sheet.charts_complete = not unread_charts
            return sheet
        except Exception as e:
            errors.record(f"Unable to fetch sheet with id {sheet_id}. Exception: {e}")
            errors.sheets_unread = True
        return None

    def _get_app_sheets(
        self,
        websocket_connection: WebsocketConnection,
        app_id: str,
        errors: ReadErrors,
        keep_private: bool = True,
    ) -> List[Sheet]:
        sheets: List[Sheet] = []
        if not self.config.include_sheets_and_charts:
            return sheets
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
                        keep_private,
                    )
                if sheet:
                    sheets.append(sheet)
            websocket_connection.add_master_measure_definitions(
                sheets, on_error=errors.record, on_fallback=errors.record_fallback
            )
        except Exception as e:
            errors.record(f"Unable to fetch sheets for app {app_id}. Exception: {e}")
            errors.sheets_unread = True
        return sheets
