from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    IntendedUseClass,
    MLModelPropertiesClass,
    OwnershipClass,
)
from datahub.metadata.urns import MlModelUrn

graph = get_default_graph()

model_urn = MlModelUrn(platform="mlflow", name="customer-churn-predictor", env="PROD")

print(f"Entity URN: {model_urn}")
print("\nAspects:")

props = graph.get_aspect(entity_urn=str(model_urn), aspect_type=MLModelPropertiesClass)
if props is None:
    raise SystemExit(f"ML model not found: {model_urn}")

print(f"  Name: {props.name}")
print(f"  Description: {props.description}")
print(f"  Type: {props.type}")

if props.hyperParams:
    print("\n  Hyperparameters:")
    for param in props.hyperParams:
        print(f"    - {param.name}: {param.value}")

if props.trainingMetrics:
    print("\n  Training Metrics:")
    for metric in props.trainingMetrics:
        print(f"    - {metric.name}: {metric.value}")

tags = graph.get_aspect(entity_urn=str(model_urn), aspect_type=GlobalTagsClass)
if tags is not None:
    print(f"\n  Tags: {[tag.tag for tag in tags.tags]}")

ownership = graph.get_aspect(entity_urn=str(model_urn), aspect_type=OwnershipClass)
if ownership is not None:
    print(f"\n  Owners: {[owner.owner for owner in ownership.owners]}")

intended = graph.get_aspect(entity_urn=str(model_urn), aspect_type=IntendedUseClass)
if intended is not None:
    print(f"\n  Primary Uses: {intended.primaryUses}")
    print(f"  Out of Scope Uses: {intended.outOfScopeUses}")
