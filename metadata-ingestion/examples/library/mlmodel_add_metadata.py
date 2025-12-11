# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.metadata.urns import CorpUserUrn, DomainUrn, MlModelUrn, TagUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

mlmodel = client.entities.get(
    MlModelUrn(platform="mlflow", name="customer-churn-predictor")
)

mlmodel.set_hyper_params(
    {
        "learning_rate": "0.1",
        "max_depth": "6",
        "n_estimators": "100",
        "subsample": "0.8",
        "colsample_bytree": "0.8",
    }
)

mlmodel.set_training_metrics(
    {
        "accuracy": "0.87",
        "precision": "0.84",
        "recall": "0.82",
        "f1_score": "0.83",
        "auc_roc": "0.91",
    }
)

mlmodel.add_owner(CorpUserUrn("data_science_team"))

mlmodel.add_tag(TagUrn("production"))
mlmodel.add_tag(TagUrn("classification"))

mlmodel.set_domain(DomainUrn("urn:li:domain:customer-analytics"))

client.entities.update(mlmodel)
