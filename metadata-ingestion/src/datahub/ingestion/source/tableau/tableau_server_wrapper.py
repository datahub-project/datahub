from tableauserverclient import Server, UserItem

from datahub.ingestion.source.tableau import tableau_constant as c


class LoggedInUser:
    user: UserItem
    _site_id: str

    def __init__(self, site_id: str, user_item: UserItem):
        self.user = user_item
        self._site_id = site_id

    def site_role(self) -> str:
        assert self.user.site_role, "site_role is not available"  # to silent the lint
        return self.user.site_role

    def user_name(self) -> str:
        assert self.user.name, "user name is not available"  # to silent the lint
        return self.user.name

    def site_id(self) -> str:
        return self._site_id

    def is_site_administrator_explorer(self):
        return self.user.site_role == c.SITE_ROLE

    @staticmethod
    def from_server(server: Server) -> "LoggedInUser":
        assert server.user_id, "make the connection with tableau"
        return LoggedInUser(server.site_id, server.users.get_by_id(server.user_id))
