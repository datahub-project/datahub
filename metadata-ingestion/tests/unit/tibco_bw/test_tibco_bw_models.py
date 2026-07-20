from datahub.ingestion.source.tibco_bw.models import (
    BwApplication,
    TciApp,
    TciSubscription,
    TibcoDeployment,
)


def test_bw_application_alias_population() -> None:
    app = BwApplication.model_validate(
        {"name": "orders", "version": "1.2", "state": "Running", "appType": "bwce"}
    )
    assert app.app_type == "bwce"
    assert app.version == "1.2"
    assert app.state == "Running"


def test_tci_app_maps_type_and_status() -> None:
    app = TciApp.model_validate(
        {"name": "sync", "type": "flogo", "status": "STARTED", "version": "3"}
    )
    assert app.app_type == "flogo"
    assert app.state == "STARTED"


def test_tci_subscription_aliases() -> None:
    sub = TciSubscription.model_validate(
        {"subscriptionId": "sub-1", "name": "Prod", "orgDisplayName": "Acme"}
    )
    assert sub.subscription_id == "sub-1"
    assert sub.organization == "Acme"


def test_deployment_enum_values() -> None:
    assert TibcoDeployment("on_prem") is TibcoDeployment.ON_PREM
    assert TibcoDeployment("cloud") is TibcoDeployment.CLOUD
