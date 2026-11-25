from typing import Dict, Optional

from datahub.ingestion.source.qlik_sense.data_classes import App
from datahub.ingestion.source.qlik_sense.lib_path import (
    _connection_key,
    _lib_connection,
)
from datahub.ingestion.source.qlik_sense.qlik_base_api import (
    UNREAD_DATA_CONNECTIONS,
    UNREAD_SPACES,
)
from datahub.ingestion.source.qlik_sense.script_lineage import (
    SPACE_SEPARATOR,
    DataConnectionInfo,
    ResolvedConnection,
    app_own_variables,
    infer_data_connection,
    is_inline_connection,
    merged_variables,
)
from datahub.ingestion.source.qlik_sense.source.entities import EntityEmitMixin
from datahub.ingestion.source.qlik_sense.source_models import _SpaceNeed, _UnseenSpace
from datahub.ingestion.source.qlik_sense.source_utils import (
    MappingT,
    _get_ignoring_case,
)


class DataConnectionMixin(EntityEmitMixin):
    def _get_data_connections(self) -> Dict[str, DataConnectionInfo]:
        if self._data_connections is None:
            self._data_connections = self.qlik_api.get_data_connections()
        return self._data_connections

    def _find_data_connection(
        self, connection: str, space_id: str
    ) -> Optional[DataConnectionInfo]:
        connections = self._get_data_connections()
        lib_connection = _lib_connection(connection)
        space_name = self.qlik_api.spaces.get(space_id)
        if self._on_cloud and space_name and not lib_connection.space:
            # Qlik Cloud looks a bare name up in the app's own space only. A
            # same-named connection elsewhere may point at another database
            # when the app's own one is not visible to the API key.
            return _get_ignoring_case(
                connections, f"{space_name}{SPACE_SEPARATOR}{lib_connection.name}"
            )
        return _get_ignoring_case(connections, _connection_key(lib_connection))

    def _mapped_connection(
        self, connection: str, space_id: str, mapping: Dict[str, MappingT]
    ) -> Optional[MappingT]:
        """A recipe mapping's entry for a connection a load script names. On
        Qlik Cloud a bare name is the app's own space's connection, so an
        entry keyed `<space>:<connection>` matches it first; a bare key
        matches the connection in any space."""
        lib_connection = _lib_connection(connection)
        # As the script spells it, then as Qlik lists it, without brackets.
        names = list(
            dict.fromkeys(
                [connection, _connection_key(lib_connection), lib_connection.name]
            )
        )
        space_name = self.qlik_api.spaces.get(space_id)
        if self._on_cloud and space_name and not lib_connection.space:
            names.insert(0, f"{space_name}{SPACE_SEPARATOR}{lib_connection.name}")
        return next(
            (
                entry
                for name in names
                if (entry := _get_ignoring_case(mapping, name)) is not None
            ),
            None,
        )

    def _resolve_script_connection(
        self, connection: Optional[str], space_id: str
    ) -> Optional[ResolvedConnection]:
        if not connection:
            return None
        configured = self._mapped_connection(
            connection, space_id, self.config.data_connection_to_platform_instance
        )
        inferred = self._find_data_connection(connection, space_id)
        # Inline ODBC/OLEDB connection strings carry the driver and database
        # themselves. A bare name says nothing about the platform behind it.
        if inferred is None and is_inline_connection(connection):
            inferred = infer_data_connection(connection, connection)
        platform = (configured.platform if configured else None) or (
            inferred.platform if inferred else None
        )
        if not platform:
            return None
        return ResolvedConnection(
            platform=platform,
            database=(configured.database if configured else None)
            or (inferred.database if inferred else None),
            default_schema=configured.default_schema if configured else None,
            env=configured.env
            if configured and "env" in configured.model_fields_set
            else self.config.env,
            platform_instance=configured.platform_instance if configured else None,
        )

    def _data_connections_unread(self) -> bool:
        return any(
            item.name == UNREAD_DATA_CONNECTIONS for item in self.qlik_api.unread_items
        )

    def _app_variables(self, app: App) -> Dict[str, str]:
        return merged_variables(self.config.script_variables, app.variables)

    def _app_own_variables(self, app: App) -> Dict[str, str]:
        return app_own_variables(self.config.script_variables, app.variables)

    def _note_unseen_space(self, space: Optional[str], need: _SpaceNeed) -> None:
        """Records a space a script or Qlik names, when the API key can't see
        it, as one the run needs access to for complete lineage."""
        if not space or self.config.on_prem is not None:
            return
        if self._listed_space_names is None:
            # Without the listing, every space would look unseen.
            if any(item.name == UNREAD_SPACES for item in self.qlik_api.unread_items):
                return
            self._listed_space_names = {
                name.lower() for name in self.qlik_api.spaces.values()
            }
        if space.lower() in self._listed_space_names:
            return
        self._unseen_spaces.setdefault(
            space.lower(), _UnseenSpace(name=space)
        ).needs.add(need)
