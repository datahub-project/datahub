from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import ActorsClass, RolePropertiesClass
from datahub.metadata.urns import RoleUrn

graph = get_default_graph()

role_urn = RoleUrn("snowflake_reader_role")

props = graph.get_aspect(entity_urn=str(role_urn), aspect_type=RolePropertiesClass)
if props is None:
    raise SystemExit(f"Role not found: {role_urn}")

print(f"Role URN: {role_urn}")
print(f"Name: {props.name}")
print(f"Description: {props.description}")
print(f"Type: {props.type}")
print(f"Request URL: {props.requestUrl}")

actors = graph.get_aspect(entity_urn=str(role_urn), aspect_type=ActorsClass)
if actors is not None:
    if actors.users:
        print(f"Users: {[u.user for u in actors.users]}")
    if actors.groups:
        print(f"Groups: {[g.group for g in actors.groups]}")
