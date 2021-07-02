@dataclass
class ModelProcessor:
    sagemaker_client: Any
    env: str
    report: SourceReport

    def get_all_models(self) -> List[Dict[str, Any]]:
        """
        List all models in SageMaker.
        """

        models = []

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_models
        paginator = self.sagemaker_client.get_paginator("list_models")
        for page in paginator.paginate():
            models += page["Models"]

        return models

    def get_model_details(self, model_name: str) -> Dict[str, Any]:
        """
        Get details of a model.
        """

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_model
        return self.sagemaker_client.describe_model(ModelName=model_name)
