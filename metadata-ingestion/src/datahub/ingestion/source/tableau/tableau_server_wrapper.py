from dataclasses import dataclass

from tableauserverclient import Server, UserItem

from datahub.ingestion.source.tableau import tableau_constant as c


@dataclass
class UserInfo:
    user_name: str
    site_role: str
    site_id: str

    def has_site_administrator_explorer_privileges(self):
        return self.site_role in [
            c.ROLE_SITE_ADMIN_EXPLORER,
            c.ROLE_SITE_ADMIN_CREATOR,
            c.ROLE_SERVER_ADMIN,
        ]

    @staticmethod
    def from_server(server: Server) -> "UserInfo":
        assert server.user_id, "make the connection with tableau"

        user: UserItem = server.users.get_by_id(server.user_id)

        assert user.site_role, "site_role is not available"  # to silent the lint

        assert user.name, "user name is not available"  # to silent the lint

        assert server.site_id, "site identifier is not available"  # to silent the lint

        return UserInfo(
            user_name=user.name,
            site_role=user.site_role,
            site_id=server.site_id,
        )
