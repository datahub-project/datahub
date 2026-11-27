from datahub.ingestion.source.qlik_sense.constants import (
    APPS_READ_WITHOUT_DATA_TITLE,
    CHANGE_CHECK_FAILED_TITLE,
    CHECKPOINT_LOAD_FAILURES_TITLE,
    COLUMN_LINEAGE_READ_FAILED_TITLE,
    DATASET_LINEAGE_READ_FAILED_TITLE,
    ENGINE_REFUSED_CONNECTIONS_TITLE,
    INCLUDE_READ_FAILED_TITLE,
    OWNER_LOOKUP_FAILED_TITLE,
    QLIK_READ_FAILED_TITLE,
    SHARES_NAMED_FROM_CONNECTIONS_TITLE,
    UNRESOLVED_FILE_CONNECTIONS_TITLE,
    UNSEEN_SPACES_TITLE,
    UPSTREAM_GRAPH_READ_FAILED_TITLE,
)
from datahub.ingestion.source.qlik_sense.lib_path import path_space
from datahub.ingestion.source.qlik_sense.report_text import (
    bounded_list,
)
from datahub.ingestion.source.qlik_sense.source.items import ItemsMixin
from datahub.ingestion.source.qlik_sense.source_models import SpaceNeed


class EndOfRunMixin(ItemsMixin):
    def _record_end_of_run_urns(self) -> None:
        for app_id, urns in self._app_end_of_run_urns.items():
            self.app_state.add_urns(app_id, urns)
        for app_id, urns in self._app_read_unmapped_files.items():
            self.app_state.add_read_unmapped_files(app_id, urns)
        for app_id, urns in self._app_read_graph_named.items():
            self.app_state.add_read_graph_named(app_id, urns)

    def _report_checkpoint_load_failures(self) -> None:
        if not self._checkpoint_load_failures:
            return
        self.reporter.warning(
            title=CHECKPOINT_LOAD_FAILURES_TITLE,
            message="Lineage that these apps' last reads recorded is left out of "
            "this run and of the checkpoint it saves, so it may not be removed "
            "once it goes stale.",
            # Every lookup of an app that fails adds the same entry.
            context=bounded_list(list(dict.fromkeys(self._checkpoint_load_failures))),
        )

    def _report_end_of_run(self) -> None:
        if self.qlik_api.opened_without_data:
            self.reporter.warning(
                title=APPS_READ_WITHOUT_DATA_TITLE,
                message="Opening these apps with their data, or reading their "
                "data model, was denied, as with section access, or timed out, so "
                "they were read without it: their tables have no field types, and "
                "the next run reads them again.",
                context=bounded_list(self.qlik_api.opened_without_data),
            )
        refusals = self.qlik_api.engine_refusals
        if refusals is not None and refusals.longest_streak_seconds > 0:
            self.reporter.info(
                title=ENGINE_REFUSED_CONNECTIONS_TITLE,
                message="App reads waited for the Engine to accept connections, "
                "as it does while its service restarts, which slows the run.",
                context=f"Longest run of refusals: "
                f"{refusals.longest_streak_seconds:.0f}s",
            )
        if self.qlik_api.field_lineage_failures:
            self.reporter.warning(
                title=COLUMN_LINEAGE_READ_FAILED_TITLE,
                message="Column lineage of these tables is only added to this run, "
                "and their table lineage is still read.",
                context=bounded_list(self.qlik_api.field_lineage_failures),
            )
        for failure in self.qlik_api.include_read_failures:
            # Entries are `<path> (app <id> in space <name>): <reason>`, and a
            # bracketed space name in the path may hold `: ` itself.
            path = failure.partition(" (app ")[0]
            self._note_unseen_space(path_space(path), SpaceNeed.INCLUDED_SCRIPTS)
        if self._unseen_spaces:
            self.reporter.warning(
                title=UNSEEN_SPACES_TITLE,
                message="Lineage goes through these spaces, which the API key's "
                "user can't see, so it is incomplete: included scripts in them "
                "are not read, their data connections don't resolve, and their "
                "datasets are named only from Qlik's lineage graph or left out. "
                "Give the user Can view in each space for its scripts and "
                "datasets, and Can consume data for its data connections, or map "
                "the connections in `data_connection_to_platform_instance` and "
                "`file_connection_to_storage`.",
                context=bounded_list(
                    sorted(
                        f"{space.name}: {', '.join(sorted(space.needs))}"
                        for space in self._unseen_spaces.values()
                    )
                ),
            )
        if self.qlik_api.include_read_failures:
            self.reporter.warning(
                title=INCLUDE_READ_FAILED_TITLE,
                message="Apps include these scripts with $(Include) or "
                "$(Must_Include), but they could not be read, so what they load "
                "is missing from lineage, and the apps' lineage is only added to. "
                "Each entry gives the path, the app and space including it, and "
                "the reason. A path without a space resolves in that app's space. "
                "An access error needs Can view for the API key's user in the "
                "space holding the file.",
                context=bounded_list(sorted(set(self.qlik_api.include_read_failures))),
            )
        if self.reporter.unresolved_file_connections:
            self.reporter.warning(
                title=UNRESOLVED_FILE_CONNECTIONS_TITLE,
                message="Files on these data connections are named by their "
                "space, connection and path, so they only join the Qlik apps "
                "that write and read them. A personal-space app's files on a "
                "connection named without its space are named from the loading "
                "app's lineage graph, when its tables come from the graph, and "
                "stores into them are not linked. Add each Azure Storage, Amazon "
                "S3 or Google Cloud Storage connection to "
                "`file_connection_to_storage` with the URI of its storage; other "
                "connectors, such as SFTP, can't be mapped.",
                context=bounded_list(sorted(self.reporter.unresolved_file_connections)),
            )
        if self.reporter.shares_named_from_connections:
            self.reporter.info(
                title=SHARES_NAMED_FROM_CONNECTIONS_TITLE,
                message="These Data Gateway connections' entries in "
                "`file_connection_to_storage` set no `storage_uri`, so their "
                "files are named from the share path the connection holds. If "
                "the on-prem recipe's `qvd_folder_aliases` renames that share, "
                "or on-prem apps reach it by another host name or a drive, set "
                "`storage_uri` to the share as the on-prem recipe names it, or "
                "lineage between the two recipes does not join.",
                context=bounded_list(
                    sorted(self.reporter.shares_named_from_connections)
                ),
            )
        if self.qlik_api.lineage_read_failures:
            self.reporter.warning(
                title=DATASET_LINEAGE_READ_FAILED_TITLE,
                message="Lineage from the apps that store these datasets is skipped.",
                context=bounded_list(self.qlik_api.lineage_read_failures),
            )
        self._report_reload_sql()
        if self._upstream_graph_read_failures:
            self.reporter.warning(
                title=UPSTREAM_GRAPH_READ_FAILED_TITLE,
                message="Sources of these apps that only Qlik's lineage graph "
                "names, such as datasets in spaces the API key can't see, are "
                "left out, and the apps' lineage is only added to this run. "
                "Datasets the last run named from the graph are kept.",
                context=bounded_list(self._upstream_graph_read_failures),
            )
        change_check_failures = [
            *self.qlik_api.change_check_failures,
            *self._change_check_failures,
        ]
        for failure in change_check_failures:
            self.reporter.apps_change_check_failed.append(failure)
        if change_check_failures:
            # Reported here rather than from the worker threads that check apps.
            self.reporter.warning(
                title=CHANGE_CHECK_FAILED_TITLE,
                message="These apps were read in full instead of skipped. "
                "See apps_change_check_failed.",
                context=f"{len(change_check_failures)} apps, for example "
                f"{change_check_failures[0]}",
            )
        self._report_checkpoint_load_failures()
        failed_users = self.qlik_api.failed_user_lookups
        if failed_users:
            self.reporter.warning(
                title=OWNER_LOOKUP_FAILED_TITLE,
                message="Ownership is left out for entities owned by these users.",
                context=", ".join(
                    f"{user_id}: {cause}"
                    for user_id, cause in sorted(failed_users.items())
                ),
            )
        for unread in self.qlik_api.unread_items:
            # Also stops stale entity removal from soft-deleting what was not read.
            self.reporter.failure(
                title=QLIK_READ_FAILED_TITLE,
                message="Left out of this run after a request failed.",
                context=f"{unread.name}: {unread.cause}",
            )
