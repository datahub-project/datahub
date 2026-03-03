import datahub.metadata.schema_classes as models
from datahub.metadata.urns import DatasetUrn, MlModelUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

model_urn = MlModelUrn(platform="mlflow", name="customer-churn-predictor")

mlmodel = client.entities.get(model_urn)

intended_use = models.IntendedUseClass(
    primaryUses=[
        "Predict customer churn to enable proactive retention campaigns",
        "Identify high-risk customers for targeted interventions",
    ],
    primaryUsers=[models.IntendedUserTypeClass.ENTERPRISE],
    outOfScopeUses=[
        "Not suitable for real-time predictions (batch inference only)",
        "Not trained on international markets outside North America",
    ],
)

mlmodel._set_aspect(intended_use)

training_data = models.TrainingDataClass(
    trainingData=[
        models.BaseDataClass(
            dataset=str(
                DatasetUrn(
                    platform="snowflake", name="prod.analytics.customer_features"
                )
            ),
            motivation="Historical customer data with confirmed churn labels",
            preProcessing=[
                "Removed customers with less than 30 days of history",
                "Standardized numerical features using StandardScaler",
                "One-hot encoded categorical variables",
            ],
        )
    ]
)

mlmodel._set_aspect(training_data)

source_code = models.SourceCodeClass(
    sourceCode=[
        models.SourceCodeUrlClass(
            type=models.SourceCodeUrlTypeClass.ML_MODEL_SOURCE_CODE,
            sourceCodeUrl="https://github.com/example/ml-models/tree/main/churn-predictor",
        )
    ]
)

mlmodel._set_aspect(source_code)

ethical_considerations = models.EthicalConsiderationsClass(
    data=["Model uses demographic data (age, location) which may be sensitive"],
    risksAndHarms=[
        "Predictions may disproportionately affect certain customer segments",
        "False positives could lead to unnecessary retention spending",
    ],
    mitigations=[
        "Regular bias audits conducted quarterly",
        "Human review required for high-value customer interventions",
    ],
)

mlmodel._set_aspect(ethical_considerations)

client.entities.update(mlmodel)

print(f"Updated aspects for model: {model_urn}")
