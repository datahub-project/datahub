import logging

from datahub.cli import delete_cli
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.rest_emitter import DatahubRestEmitter

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
dataset_urn = make_dataset_urn(name="fct_users_created", platform="hive")

delete_cli._delete_one_urn(urn=dataset_urn, soft=True, cached_emitter=rest_emitter)

log.info(f"Deleted dataset {dataset_urn}")
