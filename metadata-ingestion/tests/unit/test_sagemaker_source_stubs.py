from datetime import datetime, timezone
from typing import Any, Mapping

list_feature_groups_response = {
    "FeatureGroupSummaries": [
        {
            "FeatureGroupName": "test-2",
            "FeatureGroupArn": "arn:aws:sagemaker:us-west-2:123412341234:feature-group/test-2",
            "CreationTime": datetime(2021, 6, 24, 9, 48, 37, 35000),
            "FeatureGroupStatus": "Created",
        },
        {
            "FeatureGroupName": "test-1",
            "FeatureGroupArn": "arn:aws:sagemaker:us-west-2:123412341234:feature-group/test-1",
            "CreationTime": datetime(2021, 6, 23, 13, 58, 10, 264000),
            "FeatureGroupStatus": "Created",
        },
        {
            "FeatureGroupName": "test",
            "FeatureGroupArn": "arn:aws:sagemaker:us-west-2:123412341234:feature-group/test",
            "CreationTime": datetime(2021, 6, 14, 11, 3, 0, 803000),
            "FeatureGroupStatus": "Created",
        },
    ],
    "NextToken": "",
}
describe_feature_group_response_1 = {
    "FeatureGroupArn": "arn:aws:sagemaker:us-west-2:123412341234:feature-group/test-2",
    "FeatureGroupName": "test-2",
    "RecordIdentifierFeatureName": "some-feature-2",
    "EventTimeFeatureName": "some-feature-3",
    "FeatureDefinitions": [
        {"FeatureName": "some-feature-1", "FeatureType": "String"},
        {"FeatureName": "some-feature-2", "FeatureType": "Integral"},
        {"FeatureName": "some-feature-3", "FeatureType": "Fractional"},
    ],
    "CreationTime": datetime(2021, 6, 24, 9, 48, 37, 35000),
    "OnlineStoreConfig": {"EnableOnlineStore": True},
    "OfflineStoreConfig": {
        "S3StorageConfig": {
            "S3Uri": "s3://datahub-sagemaker-outputs",
            "ResolvedOutputS3Uri": "s3://datahub-sagemaker-outputs/123412341234/sagemaker/us-west-2/offline-store/test-2-123412341234/data",
        },
        "DisableGlueTableCreation": False,
        "DataCatalogConfig": {
            "TableName": "test-2-123412341234",
            "Catalog": "AwsDataCatalog",
            "Database": "sagemaker_featurestore",
        },
    },
    "RoleArn": "arn:aws:iam::123412341234:role/service-role/AmazonSageMaker-ExecutionRole-20210614T104201",
    "FeatureGroupStatus": "Created",
    "Description": "Yet another test feature group",
    "NextToken": "",
}
describe_feature_group_response_2 = {
    "FeatureGroupArn": "arn:aws:sagemaker:us-west-2:123412341234:feature-group/test-1",
    "FeatureGroupName": "test-1",
    "RecordIdentifierFeatureName": "id",
    "EventTimeFeatureName": "time",
    "FeatureDefinitions": [
        {"FeatureName": "name", "FeatureType": "String"},
        {"FeatureName": "id", "FeatureType": "Integral"},
        {"FeatureName": "height", "FeatureType": "Fractional"},
        {"FeatureName": "time", "FeatureType": "String"},
    ],
    "CreationTime": datetime(2021, 6, 23, 13, 58, 10, 264000),
    "OnlineStoreConfig": {"EnableOnlineStore": True},
    "FeatureGroupStatus": "Created",
    "Description": "First test feature group",
    "NextToken": "",
}
describe_feature_group_response_3 = {
    "FeatureGroupArn": "arn:aws:sagemaker:us-west-2:123412341234:feature-group/test",
    "FeatureGroupName": "test",
    "RecordIdentifierFeatureName": "feature_1",
    "EventTimeFeatureName": "feature_3",
    "FeatureDefinitions": [
        {"FeatureName": "feature_1", "FeatureType": "String"},
        {"FeatureName": "feature_2", "FeatureType": "Integral"},
        {"FeatureName": "feature_3", "FeatureType": "Fractional"},
    ],
    "CreationTime": datetime(
        2021,
        6,
        14,
        11,
        3,
        0,
        803000,
    ),
    "OnlineStoreConfig": {"EnableOnlineStore": True},
    "FeatureGroupStatus": "Created",
    "NextToken": "",
}

auto_ml_job_name = "an-auto-ml-job"
auto_ml_job_arn = "arn:aws:sagemaker:us-west-2:123412341234:auto-ml-job/an-auto-ml-job"
list_auto_ml_jobs_response = {
    "AutoMLJobSummaries": [
        {
            "AutoMLJobName": auto_ml_job_name,
            "AutoMLJobArn": auto_ml_job_arn,
            "AutoMLJobStatus": "Completed",
            "AutoMLJobSecondaryStatus": "Starting",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "EndTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "FailureReason": "string",
            "PartialFailureReasons": [
                {"PartialFailureMessage": "string"},
            ],
        },
    ],
}
describe_auto_ml_job_response = {
    "AutoMLJobName": auto_ml_job_name,
    "AutoMLJobArn": auto_ml_job_arn,
    "AutoMLJobInputDataConfig": [
        {
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "ManifestFile",  # 'ManifestFile'|'S3Prefix'
                    "S3Uri": "s3://auto-ml-job-input-bucket/file.txt",
                }
            },
            "CompressionType": "None",  # 'None'|'Gzip'
        },
    ],
    "OutputDataConfig": {
        "KmsKeyId": "some-key-id",
        "S3OutputPath": "s3://auto-ml-job-output-bucket/file.txt",
    },
    "RoleArn": "arn:aws:iam::123412341234:role/service-role/AmazonSageMakerServiceCatalogProductsUseRole",
    "ResolvedAttributes": {
        "AutoMLJobObjective": {
            "MetricName": "Accuracy",  # 'Accuracy'|'MSE'|'F1'|'F1macro'|'AUC'
        },
        "CompletionCriteria": {
            "MaxCandidates": 123,
            "MaxRuntimePerTrainingJobInSeconds": 123,
            "MaxAutoMLJobRuntimeInSeconds": 123,
        },
        "AutoMLProblemTypeResolvedAttributes": {
            "TabularResolvedAttributes": {
                "ProblemType": "BinaryClassification",  # 'BinaryClassification'|'MulticlassClassification'|'Regression'
            }
        },
    },
    "SecurityConfig": {
        "VolumeKmsKeyId": "string",
        "EnableInterContainerTrafficEncryption": True,  # True|False
        "VpcConfig": {
            "SecurityGroupIds": [
                "string",
            ],
            "Subnets": [
                "string",
            ],
        },
    },
    "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "EndTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "FailureReason": "string",
    "PartialFailureReasons": [
        {"PartialFailureMessage": "string"},
    ],
    "BestCandidate": {
        "CandidateName": "string",
        "FinalAutoMLJobObjectiveMetric": {
            "Type": "Maximize",  # "Maximize" | "Minimize"
            "MetricName": "Accuracy",  # "Accuracy" | "MSE" | "F1" | "F1macro" | "AUC"
            "Value": 1.0,
        },
        "ObjectiveStatus": "Succeeded",  # "Succeeded" | "Pending" | "Failed"
        "CandidateSteps": [
            {
                "CandidateStepType": "AWS::SageMaker::TrainingJob",
                # "AWS::SageMaker::TrainingJob"
                # | "AWS::SageMaker::TransformJob"
                # | "AWS::SageMaker::ProcessingJob",
                "CandidateStepArn": "string",
                "CandidateStepName": "string",
            },
        ],
        "CandidateStatus": "Completed",
        # "Completed"
        # | "InProgress"
        # | "Failed"
        # | "Stopped"
        # | "Stopping"
        "InferenceContainers": [
            {
                "Image": "string",
                "ModelDataUrl": "s3://auto-ml-job/model-artifact.tar.gz",
                "Environment": {"string": "string"},
            },
        ],
        "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        "EndTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        "FailureReason": "string",
        "CandidateProperties": {
            "CandidateArtifactLocations": {"Explainability": "string"}
        },
    },
    "AutoMLJobStatus": "Completed",  # "Completed" | "InProgress" | "Failed" | "Stopped" | "Stopping"
    "AutoMLJobSecondaryStatus": "Starting",
    # "Starting"
    # | "AnalyzingData"
    # | "FeatureEngineering"
    # | "ModelTuning"
    # | "MaxCandidatesReached"
    # | "Failed"
    # | "Stopped"
    # | "MaxAutoMLJobRuntimeReached"
    # | "Stopping"
    # | "CandidateDefinitionsGenerated"
    # | "GeneratingExplainabilityReport"
    # | "Completed"
    # | "ExplainabilityError"
    # | "DeployingModel"
    # | "ModelDeploymentError"
    "AutoMLProblemTypeConfig": {
        "TabularJobConfig": {
            "CandidateGenerationConfig": {
                "AlgorithmsConfig": [
                    {
                        "AutoMLAlgorithms": [
                            "xgboost",  # 'xgboost'|'linear-learner'|'mlp'|'lightgbm'|'catboost'|'randomforest'|'extra-trees'|'nn-torch'|'fastai'
                        ]
                    },
                ]
            },
            "CompletionCriteria": {
                "MaxCandidates": 123,
                "MaxRuntimePerTrainingJobInSeconds": 123,
                "MaxAutoMLJobRuntimeInSeconds": 123,
            },
            "FeatureSpecificationS3Uri": "string",
            "Mode": "AUTO",  # "AUTO" | "ENSEMBLING" | "HYPERPARAMETER_TUNING",
            "GenerateCandidateDefinitionsOnly": True,  # True | False,
            "ProblemType": "BinaryClassification",
            # "BinaryClassification"
            # | "MulticlassClassification"
            # | "Regression",
            "TargetAttributeName": "ChannelType",  # ChannelType, ContentType, CompressionType, DataSource
            "SampleWeightAttributeName": "string",
        }
    },
    "AutoMLJobArtifacts": {
        "CandidateDefinitionNotebookLocation": "string",
        "DataExplorationNotebookLocation": "string",
    },
    "ModelDeployConfig": {
        "AutoGenerateEndpointName": True,  # True | False
        "EndpointName": "string",
    },
    "ModelDeployResult": {"EndpointName": "string"},
}

compilation_job_name = "a-compilation-job"
compilation_job_arn = (
    "arn:aws:sagemaker:us-west-2:123412341234:compilation-job/a-compilation-job"
)
list_compilation_jobs_response = {
    "CompilationJobSummaries": [
        {
            "CompilationJobName": compilation_job_name,
            "CompilationJobArn": compilation_job_arn,
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "CompilationStartTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "CompilationEndTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "CompilationTargetDevice": "lambda",
            "CompilationTargetPlatformOs": "ANDROID",
            "CompilationTargetPlatformArch": "X86_64",
            "CompilationTargetPlatformAccelerator": "INTEL_GRAPHICS",
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "CompilationJobStatus": "INPROGRESS",
        },
    ],
}
describe_compilation_job_response = {
    "CompilationJobName": compilation_job_name,
    "CompilationJobArn": compilation_job_arn,
    "CompilationJobStatus": "INPROGRESS",  # 'INPROGRESS'|'COMPLETED'|'FAILED'|'STARTING'|'STOPPING'|'STOPPED'
    "CompilationStartTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "CompilationEndTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "StoppingCondition": {"MaxRuntimeInSeconds": 123, "MaxWaitTimeInSeconds": 123},
    "InferenceImage": "string",
    "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "FailureReason": "string",
    "ModelArtifacts": {
        "S3ModelArtifacts": "s3://compilation-job-bucket/model-artifacts.tar.gz"
    },
    "ModelDigests": {"ArtifactDigest": "string"},
    "RoleArn": "arn:aws:iam::123412341234:role/service-role/AmazonSageMakerServiceCatalogProductsUseRole",
    "InputConfig": {
        "S3Uri": "s3://compilation-job-bucket/input-config.tar.gz",
        "DataInputConfig": "string",
        "Framework": "TENSORFLOW",  # 'TENSORFLOW'|'KERAS'|'MXNET'|'ONNX'|'PYTORCH'|'XGBOOST'|'TFLITE'|'DARKNET'|'SKLEARN'
        "FrameworkVersion": "string",
    },
    "OutputConfig": {
        "S3OutputLocation": "s3://compilation-job-bucket/output-config.tar.gz",
        "TargetDevice": "lambda",
        "TargetPlatform": {
            "Os": "ANDROID",  # 'ANDROID'|'LINUX'
            "Arch": "X86_64",  # 'X86_64'|'X86'|'ARM64'|'ARM_EABI'|'ARM_EABIHF'
            "Accelerator": "INTEL_GRAPHICS",  # 'INTEL_GRAPHICS'|'MALI'|'NVIDIA'
        },
        "CompilerOptions": "string",
        "KmsKeyId": "string",
    },
    "VpcConfig": {
        "SecurityGroupIds": [
            "string",
        ],
        "Subnets": [
            "string",
        ],
    },
}


hyper_parameter_tuning_job_name = "a-hyper-parameter-tuning-job"
hyper_parameter_tuning_job_arn = "arn:aws:sagemaker:us-west-2:123412341234:hyper-parameter-tuning-job/a-hyper-parameter-tuning-job"
list_hyper_parameter_tuning_jobs_response = {
    "HyperParameterTuningJobSummaries": [
        {
            "HyperParameterTuningJobName": hyper_parameter_tuning_job_name,
            "HyperParameterTuningJobArn": hyper_parameter_tuning_job_arn,
            "HyperParameterTuningJobStatus": "Completed",
            "Strategy": "Bayesian",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "HyperParameterTuningEndTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "TrainingJobStatusCounters": {
                "Completed": 123,
                "InProgress": 123,
                "RetryableError": 123,
                "NonRetryableError": 123,
                "Stopped": 123,
            },
            "ObjectiveStatusCounters": {
                "Succeeded": 123,
                "Pending": 123,
                "Failed": 123,
            },
            "ResourceLimits": {
                "MaxNumberOfTrainingJobs": 123,
                "MaxParallelTrainingJobs": 123,
            },
        },
    ],
}
describe_hyper_parameter_tuning_job_response = {
    "HyperParameterTuningJobName": hyper_parameter_tuning_job_name,
    "HyperParameterTuningJobArn": hyper_parameter_tuning_job_arn,
    "HyperParameterTuningJobConfig": {
        "Strategy": "Bayesian",  # 'Bayesian'|'Random'
        "HyperParameterTuningJobObjective": {
            "Type": "Maximize",  # 'Maximize'|'Minimize'
            "MetricName": "string",
        },
        "ResourceLimits": {
            "MaxNumberOfTrainingJobs": 123,
            "MaxParallelTrainingJobs": 123,
        },
        "ParameterRanges": {
            "IntegerParameterRanges": [
                {
                    "Name": "string",
                    "MinValue": "string",
                    "MaxValue": "string",
                    "ScalingType": "Auto",  # 'Auto'|'Linear'|'Logarithmic'|'ReverseLogarithmic'
                },
            ],
            "ContinuousParameterRanges": [
                {
                    "Name": "string",
                    "MinValue": "string",
                    "MaxValue": "string",
                    "ScalingType": "Auto",  # 'Auto'|'Linear'|'Logarithmic'|'ReverseLogarithmic'
                },
            ],
            "CategoricalParameterRanges": [
                {
                    "Name": "string",
                    "Values": [
                        "string",
                    ],
                },
            ],
        },
        "TrainingJobEarlyStoppingType": "Off",  # 'Off'|'Auto'
        "TuningJobCompletionCriteria": {"TargetObjectiveMetricValue": 1.0},
    },
    "TrainingJobDefinition": {
        "DefinitionName": "string",
        "TuningObjective": {
            "Type": "Maximize",  # "Maximize" | "Minimize"
            "MetricName": "string",
        },
        "HyperParameterRanges": {
            "IntegerParameterRanges": [
                {
                    "Name": "string",
                    "MinValue": "string",
                    "MaxValue": "string",
                    "ScalingType": "Auto",  # 'Auto'|'Linear'|'Logarithmic'|'ReverseLogarithmic'
                },
            ],
            "ContinuousParameterRanges": [
                {
                    "Name": "string",
                    "MinValue": "string",
                    "MaxValue": "string",
                    "ScalingType": "Auto",  # 'Auto'|'Linear'|'Logarithmic'|'ReverseLogarithmic'
                },
            ],
            "CategoricalParameterRanges": [
                {
                    "Name": "string",
                    "Values": [
                        "string",
                    ],
                },
            ],
        },
        "StaticHyperParameters": {"string": "string"},
        "AlgorithmSpecification": {
            "TrainingImage": "string",
            "TrainingInputMode": "Pipe",  # 'Pipe'|'File'
            "AlgorithmName": "string",
            "MetricDefinitions": [
                {"Name": "string", "Regex": "string"},
            ],
        },
        "RoleArn": "arn:aws:iam::123412341234:role/service-role/AmazonSageMakerServiceCatalogProductsUseRole",
        "InputDataConfig": [
            {
                "ChannelName": "string",
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "ManifestFile",  # 'ManifestFile'|'S3Prefix'|'AugmentedManifestFile'
                        "S3Uri": "s3://hyper-parameter-tuning-job/data-source.tar.gz",
                        "S3DataDistributionType": "FullyReplicated",  # 'FullyReplicated'|'ShardedByS3Key'
                        "AttributeNames": [
                            "string",
                        ],
                    },
                    "FileSystemDataSource": {
                        "FileSystemId": "abcdefgihjklmnopqrstuvwxyz",
                        "FileSystemAccessMode": "rw",  # 'rw'|'ro'
                        "FileSystemType": "EFS",  # 'EFS'|'FSxLustre'
                        "DirectoryPath": "string",
                    },
                },
                "ContentType": "string",
                "CompressionType": "None",  # 'None'|'Gzip'
                "RecordWrapperType": "None",  # 'None'|'RecordIO'
                "InputMode": "Pipe",  # 'Pipe'|'File'
                "ShuffleConfig": {"Seed": 123},
            },
        ],
        "VpcConfig": {
            "SecurityGroupIds": [
                "string",
            ],
            "Subnets": [
                "string",
            ],
        },
        "OutputDataConfig": {
            "KmsKeyId": "string",
            "S3OutputPath": "s3://hyper-parameter-tuning-job/data-output.tar.gz",
        },
        "ResourceConfig": {
            "InstanceType": "ml.m4.xlarge",
            "InstanceCount": 123,
            "VolumeSizeInGB": 123,
            "VolumeKmsKeyId": "string",
        },
        "StoppingCondition": {"MaxRuntimeInSeconds": 123, "MaxWaitTimeInSeconds": 123},
        "EnableNetworkIsolation": True,  # True|False
        "EnableInterContainerTrafficEncryption": True,  # True|False
        "EnableManagedSpotTraining": True,  # True|False
        "CheckpointConfig": {
            "S3Uri": "s3://hyper-parameter-tuning-job/checkpoint-config.tar.gz",
            "LocalPath": "string",
        },
        "RetryStrategy": {"MaximumRetryAttempts": 123},
    },
    "TrainingJobDefinitions": [
        {
            "DefinitionName": "string",
            "TuningObjective": {
                "Type": "Maximize",  # 'Maximize'|'Minimize'
                "MetricName": "string",
            },
            "HyperParameterRanges": {
                "IntegerParameterRanges": [
                    {
                        "Name": "string",
                        "MinValue": "string",
                        "MaxValue": "string",
                        "ScalingType": "Auto",  # 'Auto'|'Linear'|'Logarithmic'|'ReverseLogarithmic'
                    },
                ],
                "ContinuousParameterRanges": [
                    {
                        "Name": "string",
                        "MinValue": "string",
                        "MaxValue": "string",
                        "ScalingType": "Auto",  # 'Auto'|'Linear'|'Logarithmic'|'ReverseLogarithmic'
                    },
                ],
                "CategoricalParameterRanges": [
                    {
                        "Name": "string",
                        "Values": [
                            "string",
                        ],
                    },
                ],
            },
            "StaticHyperParameters": {"string": "string"},
            "AlgorithmSpecification": {
                "TrainingImage": "string",
                "TrainingInputMode": "Pipe",  # 'Pipe'|'File'
                "AlgorithmName": "string",
                "MetricDefinitions": [
                    {"Name": "string", "Regex": "string"},
                ],
            },
            "RoleArn": "arn:aws:iam::123412341234:role/service-role/AmazonSageMakerServiceCatalogProductsUseRole",
            "InputDataConfig": [
                {
                    "ChannelName": "string",
                    "DataSource": {
                        "S3DataSource": {
                            "S3DataType": "ManifestFile",  # 'ManifestFile'|'S3Prefix'|'AugmentedManifestFile'
                            "S3Uri": "s3://hyper-parameter-tuning-job/data-source.tar.gz",
                            "S3DataDistributionType": "FullyReplicated",  # 'FullyReplicated'|'ShardedByS3Key'
                            "AttributeNames": [
                                "string",
                            ],
                        },
                        "FileSystemDataSource": {
                            "FileSystemId": "abcdefgihjklmnopqrstuvwxyz",
                            "FileSystemAccessMode": "rw",  # 'rw'|'ro'
                            "FileSystemType": "EFS",  # 'EFS'|'FSxLustre'
                            "DirectoryPath": "string",
                        },
                    },
                    "ContentType": "string",
                    "CompressionType": "None",  # 'None'|'Gzip'
                    "RecordWrapperType": "None",  # 'None'|'RecordIO'
                    "InputMode": "Pipe",  # 'Pipe'|'File'
                    "ShuffleConfig": {"Seed": 123},
                },
            ],
            "VpcConfig": {
                "SecurityGroupIds": [
                    "string",
                ],
                "Subnets": [
                    "string",
                ],
            },
            "OutputDataConfig": {
                "KmsKeyId": "string",
                "S3OutputPath": "s3://hyper-parameter-tuning-job/data-output.tar.gz",
            },
            "ResourceConfig": {
                "InstanceType": "ml.m4.xlarge",
                "InstanceCount": 123,
                "VolumeSizeInGB": 123,
                "VolumeKmsKeyId": "string",
            },
            "StoppingCondition": {
                "MaxRuntimeInSeconds": 123,
                "MaxWaitTimeInSeconds": 123,
            },
            "EnableNetworkIsolation": True,  # True|False
            "EnableInterContainerTrafficEncryption": True,  # True|False
            "EnableManagedSpotTraining": True,  # True|False
            "CheckpointConfig": {
                "S3Uri": "s3://hyper-parameter-tuning-job/checkpoint-config.tar.gz",
                "LocalPath": "string",
            },
            "RetryStrategy": {"MaximumRetryAttempts": 123},
        },
    ],
    "HyperParameterTuningJobStatus": "Completed",  # 'Completed'|'InProgress'|'Failed'|'Stopped'|'Stopping'
    "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "HyperParameterTuningEndTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "TrainingJobStatusCounters": {
        "Completed": 123,
        "InProgress": 123,
        "RetryableError": 123,
        "NonRetryableError": 123,
        "Stopped": 123,
    },
    "ObjectiveStatusCounters": {"Succeeded": 123, "Pending": 123, "Failed": 123},
    "BestTrainingJob": {
        "TrainingJobDefinitionName": "string",
        "TrainingJobName": "string",
        "TrainingJobArn": "string",
        "TuningJobName": "string",
        "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        "TrainingStartTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        "TrainingEndTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        "TrainingJobStatus": "InProgress",  # 'InProgress'|'Completed'|'Failed'|'Stopping'|'Stopped'
        "TunedHyperParameters": {"string": "string"},
        "FailureReason": "string",
        "FinalHyperParameterTuningJobObjectiveMetric": {
            "Type": "Maximize",  # 'Maximize'|'Minimize'
            "MetricName": "string",
            "Value": 1.0,
        },
        "ObjectiveStatus": "Succeeded",  # 'Succeeded'|'Pending'|'Failed'
    },
    "OverallBestTrainingJob": {
        "TrainingJobDefinitionName": "string",
        "TrainingJobName": "string",
        "TrainingJobArn": "string",
        "TuningJobName": "string",
        "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        "TrainingStartTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        "TrainingEndTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        "TrainingJobStatus": "InProgress",  # 'InProgress'|'Completed'|'Failed'|'Stopping'|'Stopped'
        "TunedHyperParameters": {"string": "string"},
        "FailureReason": "string",
        "FinalHyperParameterTuningJobObjectiveMetric": {
            "Type": "Maximize",  # 'Maximize'|'Minimize'
            "MetricName": "string",
            "Value": 1.0,
        },
        "ObjectiveStatus": "Succeeded",  # 'Succeeded'|'Pending'|'Failed'
    },
    "WarmStartConfig": {
        "ParentHyperParameterTuningJobs": [
            {"HyperParameterTuningJobName": "string"},
        ],
        "WarmStartType": "IdenticalDataAndAlgorithm",  # 'IdenticalDataAndAlgorithm'|'TransferLearning'
    },
    "FailureReason": "string",
}

labeling_job_name = "a-labeling-job"
labeling_job_arn = (
    "arn:aws:sagemaker:us-west-2:123412341234:labeling-job/a-labeling-job"
)
list_labeling_jobs_response = {
    "LabelingJobSummaryList": [
        {
            "LabelingJobName": labeling_job_name,
            "LabelingJobArn": labeling_job_arn,
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "LabelingJobStatus": "Initializing",
            "LabelCounters": {
                "TotalLabeled": 123,
                "HumanLabeled": 123,
                "MachineLabeled": 123,
                "FailedNonRetryableError": 123,
                "Unlabeled": 123,
            },
            "WorkteamArn": "string",
            "PreHumanTaskLambdaArn": "string",
            "AnnotationConsolidationLambdaArn": "string",
            "FailureReason": "string",
            "LabelingJobOutput": {
                "OutputDatasetS3Uri": "s3://labeling-job/output-dataset.tar.gz",
                "FinalActiveLearningModelArn": "arn:aws:sagemaker:us-west-2:123412341234:labeling-job/final-active-learning-model",
            },
            "InputConfig": {
                "DataSource": {
                    "S3DataSource": {"ManifestS3Uri": "string"},
                    "SnsDataSource": {"SnsTopicArn": "string"},
                },
                "DataAttributes": {
                    "ContentClassifiers": [
                        "FreeOfPersonallyIdentifiableInformation",
                        "FreeOfAdultContent",
                    ]
                },
            },
        },
    ],
}
describe_labeling_job_response = {
    "LabelingJobStatus": "Initializing",  # 'Initializing'|'InProgress'|'Completed'|'Failed'|'Stopping'|'Stopped'
    "LabelCounters": {
        "TotalLabeled": 123,
        "HumanLabeled": 123,
        "MachineLabeled": 123,
        "FailedNonRetryableError": 123,
        "Unlabeled": 123,
    },
    "FailureReason": "string",
    "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "JobReferenceCode": "string",
    "LabelingJobName": labeling_job_name,
    "LabelingJobArn": labeling_job_arn,
    "LabelAttributeName": "string",
    "InputConfig": {
        "DataSource": {
            "S3DataSource": {"ManifestS3Uri": "s3://labeling-job/data-source.tar.gz"},
            "SnsDataSource": {"SnsTopicArn": "string"},
        },
        "DataAttributes": {
            "ContentClassifiers": [
                "FreeOfPersonallyIdentifiableInformation",
                "FreeOfAdultContent",
            ]
        },
    },
    "OutputConfig": {
        "S3OutputPath": "s3://labeling-job/output-config.tar.gz",
        "KmsKeyId": "string",
        "SnsTopicArn": "string",
    },
    "RoleArn": "arn:aws:iam::123412341234:role/service-role/AmazonSageMakerServiceCatalogProductsUseRole",
    "LabelCategoryConfigS3Uri": "s3://labeling-job/category-config.tar.gz",
    "StoppingConditions": {
        "MaxHumanLabeledObjectCount": 123,
        "MaxPercentageOfInputDatasetLabeled": 123,
    },
    "LabelingJobAlgorithmsConfig": {
        "LabelingJobAlgorithmSpecificationArn": "string",
        "InitialActiveLearningModelArn": "arn:aws:sagemaker:us-west-2:123412341234:labeling-job/initial-active-learning-model",
        "LabelingJobResourceConfig": {"VolumeKmsKeyId": "string"},
    },
    "HumanTaskConfig": {
        "WorkteamArn": "string",
        "UiConfig": {
            "UiTemplateS3Uri": "s3://labeling-job/ui-config.tar.gz",
            "HumanTaskUiArn": "string",
        },
        "PreHumanTaskLambdaArn": "string",
        "TaskKeywords": [
            "string",
        ],
        "TaskTitle": "string",
        "TaskDescription": "string",
        "NumberOfHumanWorkersPerDataObject": 123,
        "TaskTimeLimitInSeconds": 123,
        "TaskAvailabilityLifetimeInSeconds": 123,
        "MaxConcurrentTaskCount": 123,
        "AnnotationConsolidationConfig": {"AnnotationConsolidationLambdaArn": "string"},
        "PublicWorkforceTaskPrice": {
            "AmountInUsd": {"Dollars": 123, "Cents": 123, "TenthFractionsOfACent": 123}
        },
    },
    "Tags": [
        {"Key": "string", "Value": "string"},
    ],
    "LabelingJobOutput": {
        "OutputDatasetS3Uri": "s3://labeling-job/output-dataset.tar.gz",
        "FinalActiveLearningModelArn": "arn:aws:sagemaker:us-west-2:123412341234:labeling-job/final-active-learning-model",
    },
}

training_job_name = "a-training-job"
training_job_arn = (
    "arn:aws:sagemaker:us-west-2:123412341234:training-job/a-training-job"
)
list_training_jobs_response = {
    "TrainingJobSummaries": [
        {
            "TrainingJobName": training_job_name,
            "TrainingJobArn": training_job_arn,
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "TrainingEndTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "TrainingJobStatus": "InProgress",
        },
    ],
}
describe_training_job_response = {
    "TrainingJobName": training_job_name,
    "TrainingJobArn": training_job_arn,
    "TuningJobArn": "string",
    "LabelingJobArn": "string",
    "AutoMLJobArn": "string",
    "ModelArtifacts": {"S3ModelArtifacts": "s3://the-first-model-data-url/data.tar.gz"},
    "TrainingJobStatus": "InProgress",  # 'InProgress'|'Completed'|'Failed'|'Stopping'|'Stopped'
    "SecondaryStatus": "Starting",  # 'Starting'|'LaunchingMLInstances'|'PreparingTrainingStack'|'Downloading'|'DownloadingTrainingImage'|'Training'|'Uploading'|'Stopping'|'Stopped'|'MaxRuntimeExceeded'|'Completed'|'Failed'|'Interrupted'|'MaxWaitTimeExceeded'|'Updating'|'Restarting'
    "FailureReason": "string",
    "HyperParameters": {"parameter-1": "some-value", "parameter-2": "another-value"},
    "AlgorithmSpecification": {
        "TrainingImage": "string",
        "AlgorithmName": "string",
        "TrainingInputMode": "Pipe",  # 'Pipe'|'File'
        "MetricDefinitions": [
            {"Name": "string", "Regex": "string"},
        ],
        "EnableSageMakerMetricsTimeSeries": True,  # True|False
    },
    "RoleArn": "arn:aws:iam::123412341234:role/service-role/AmazonSageMakerServiceCatalogProductsUseRole",
    "InputDataConfig": [
        {
            "ChannelName": "string",
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "ManifestFile",  # 'ManifestFile'|'S3Prefix'|'AugmentedManifestFile'
                    "S3Uri": "s3://training-job/input-dataset.tar.gz",
                    "S3DataDistributionType": "FullyReplicated",  # 'FullyReplicated'|'ShardedByS3Key'
                    "AttributeNames": [
                        "string",
                    ],
                },
                "FileSystemDataSource": {
                    "FileSystemId": "abcdefgihjklmnopqrstuvwxyz",
                    "FileSystemAccessMode": "rw",  # 'rw'|'ro'
                    "FileSystemType": "EFS",  # 'EFS'|'FSxLustre',
                    "DirectoryPath": "string",
                },
            },
            "ContentType": "string",
            "CompressionType": "None",  # 'None'|'Gzip'
            "RecordWrapperType": "None",  # 'None'|'RecordIO'
            "InputMode": "Pipe",  # 'Pipe'|'File'
            "ShuffleConfig": {"Seed": 123},
        },
    ],
    "OutputDataConfig": {
        "KmsKeyId": "string",
        "S3OutputPath": "s3://training-job/output-data.tar.gz",
    },
    "ResourceConfig": {
        "InstanceType": "ml.m4.xlarge",
        "InstanceCount": 123,
        "VolumeSizeInGB": 123,
        "VolumeKmsKeyId": "string",
    },
    "VpcConfig": {
        "SecurityGroupIds": [
            "string",
        ],
        "Subnets": [
            "string",
        ],
    },
    "StoppingCondition": {"MaxRuntimeInSeconds": 123, "MaxWaitTimeInSeconds": 123},
    "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "TrainingStartTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "TrainingEndTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "SecondaryStatusTransitions": [
        {
            "Status": "Starting",  # 'Starting'|'LaunchingMLInstances'|'PreparingTrainingStack'|'Downloading'|'DownloadingTrainingImage'|'Training'|'Uploading'|'Stopping'|'Stopped'|'MaxRuntimeExceeded'|'Completed'|'Failed'|'Interrupted'|'MaxWaitTimeExceeded'|'Updating'|'Restarting'
            "StartTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "EndTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "StatusMessage": "string",
        },
    ],
    "FinalMetricDataList": [
        {
            "MetricName": "some-metric",
            "Value": 1.0,
            "Timestamp": datetime(2015, 1, 1, tzinfo=timezone.utc),
        },
        {
            "MetricName": "another-metric",
            "Value": 1.0,
            "Timestamp": datetime(2015, 1, 1, tzinfo=timezone.utc),
        },
        {
            "MetricName": "some-metric",
            "Value": 0.0,
            "Timestamp": datetime(2014, 1, 1, tzinfo=timezone.utc),
        },
    ],
    "EnableNetworkIsolation": True,  # True|False
    "EnableInterContainerTrafficEncryption": True,  # True|False
    "EnableManagedSpotTraining": True,  # True|False
    "CheckpointConfig": {
        "S3Uri": "s3://training-job/checkpoint-config.tar.gz",
        "LocalPath": "string",
    },
    "TrainingTimeInSeconds": 123,
    "BillableTimeInSeconds": 123,
    "DebugHookConfig": {
        "LocalPath": "string",
        "S3OutputPath": "s3://training-job/debug-hook-config.tar.gz",
        "HookParameters": {"string": "string"},
        "CollectionConfigurations": [
            {"CollectionName": "string", "CollectionParameters": {"string": "string"}},
        ],
    },
    "ExperimentConfig": {
        "ExperimentName": "string",
        "TrialName": "string",
        "TrialComponentDisplayName": "string",
    },
    "DebugRuleConfigurations": [
        {
            "RuleConfigurationName": "string",
            "LocalPath": "string",
            "S3OutputPath": "s3://training-job/debug-rule-config.tar.gz",
            "RuleEvaluatorImage": "string",
            "InstanceType": "ml.t3.medium",
            "VolumeSizeInGB": 123,
            "RuleParameters": {"string": "string"},
        },
    ],
    "TensorBoardOutputConfig": {
        "LocalPath": "string",
        "S3OutputPath": "s3://training-job/tensorboard-output-config.tar.gz",
    },
    "DebugRuleEvaluationStatuses": [
        {
            "RuleConfigurationName": "string",
            "RuleEvaluationJobArn": "string",
            "RuleEvaluationStatus": "InProgress",  # 'InProgress'|'NoIssuesFound'|'IssuesFound'|'Error'|'Stopping'|'Stopped'
            "StatusDetails": "string",
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        },
    ],
    "ProfilerConfig": {
        "S3OutputPath": "s3://training-job/profiler-config.tar.gz",
        "ProfilingIntervalInMilliseconds": 123,
        "ProfilingParameters": {"string": "string"},
    },
    "ProfilerRuleConfigurations": [
        {
            "RuleConfigurationName": "string",
            "LocalPath": "string",
            "S3OutputPath": "s3://training-job/profiler-rule-config.tar.gz",
            "RuleEvaluatorImage": "string",
            "InstanceType": "ml.t3.medium",
            "VolumeSizeInGB": 123,
            "RuleParameters": {"string": "string"},
        },
    ],
    "ProfilerRuleEvaluationStatuses": [
        {
            "RuleConfigurationName": "string",
            "RuleEvaluationJobArn": "string",
            "RuleEvaluationStatus": "InProgress",  # 'InProgress'|'NoIssuesFound'|'IssuesFound'|'Error'|'Stopping'|'Stopped'
            "StatusDetails": "string",
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        },
    ],
    "ProfilingStatus": "Enabled",  # 'Enabled'|'Disabled'
    "RetryStrategy": {"MaximumRetryAttempts": 123},
    "Environment": {"string": "string"},
}

processing_job_name = "a-processing-job"
processing_job_arn = (
    "arn:aws:sagemaker:us-west-2:123412341234:processing-job/a-processing-job"
)
list_processing_jobs_response = {
    "ProcessingJobSummaries": [
        {
            "ProcessingJobName": processing_job_name,
            "ProcessingJobArn": processing_job_arn,
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "ProcessingEndTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "ProcessingJobStatus": "InProgress",
            "FailureReason": "string",
            "ExitMessage": "string",
        },
    ],
}
describe_processing_job_response = {
    "ProcessingJobName": processing_job_name,
    "ProcessingJobArn": processing_job_arn,
    "ProcessingInputs": [
        {
            "InputName": "string",
            "AppManaged": True,  # True|False
            "S3Input": {
                "S3Uri": "s3://processing-job/input-data.tar.gz",
                "LocalPath": "string",
                "S3DataType": "ManifestFile",  # 'ManifestFile'|'S3Prefix'
                "S3InputMode": "Pipe",  # 'Pipe'|'File'
                "S3DataDistributionType": "FullyReplicated",  # 'FullyReplicated'|'ShardedByS3Key'
                "S3CompressionType": "None",  # 'None'|'Gzip'
            },
            "DatasetDefinition": {
                "AthenaDatasetDefinition": {
                    "Catalog": "athena-catalog",
                    "Database": "athena-database",
                    "QueryString": "athena-query-string",
                    "WorkGroup": "athena-work-group",
                    "OutputS3Uri": "s3://processing-job/athena-output.tar.gz",
                    "KmsKeyId": "string",
                    "OutputFormat": "PARQUET",  # 'PARQUET'|'ORC'|'AVRO'|'JSON'|'TEXTFILE'
                    "OutputCompression": "GZIP",  # 'GZIP'|'SNAPPY'|'ZLIB'
                },
                "RedshiftDatasetDefinition": {
                    "ClusterId": "redshift-cluster",
                    "Database": "redshift-database",
                    "DbUser": "redshift-db-user",
                    "QueryString": "redshift-query-string",
                    "ClusterRoleArn": "arn:aws:sagemaker:us-west-2:123412341234:processing-job/redshift-cluster",
                    "OutputS3Uri": "s3://processing-job/redshift-output.tar.gz",
                    "KmsKeyId": "string",
                    "OutputFormat": "PARQUET",  # 'PARQUET'|'CSV'
                    "OutputCompression": "None",  # 'None'|'GZIP'|'BZIP2'|'ZSTD'|'SNAPPY'
                },
                "LocalPath": "string",
                "DataDistributionType": "FullyReplicated",  # 'FullyReplicated'|'ShardedByS3Key'
                "InputMode": "Pipe",  # 'Pipe'|'File'
            },
        },
    ],
    "ProcessingOutputConfig": {
        "Outputs": [
            {
                "OutputName": "string",
                "S3Output": {
                    "S3Uri": "s3://processing-job/processing-output.tar.gz",
                    "LocalPath": "string",
                    "S3UploadMode": "Continuous",  # 'Continuous'|'EndOfJob'
                },
                "FeatureStoreOutput": {"FeatureGroupName": "string"},
                "AppManaged": True,  # True|False
            },
        ],
        "KmsKeyId": "string",
    },
    "ProcessingResources": {
        "ClusterConfig": {
            "InstanceCount": 123,
            "InstanceType": "ml.t3.medium",
            "VolumeSizeInGB": 123,
            "VolumeKmsKeyId": "string",
        }
    },
    "StoppingCondition": {"MaxRuntimeInSeconds": 123},
    "AppSpecification": {
        "ImageUri": "string",
        "ContainerEntrypoint": [
            "string",
        ],
        "ContainerArguments": [
            "string",
        ],
    },
    "Environment": {"string": "string"},
    "NetworkConfig": {
        "EnableInterContainerTrafficEncryption": True,  # True|False
        "EnableNetworkIsolation": True,  # True|False
        "VpcConfig": {
            "SecurityGroupIds": [
                "string",
            ],
            "Subnets": [
                "string",
            ],
        },
    },
    "RoleArn": "arn:aws:iam::123412341234:role/service-role/AmazonSageMakerServiceCatalogProductsUseRole",
    "ExperimentConfig": {
        "ExperimentName": "string",
        "TrialName": "string",
        "TrialComponentDisplayName": "string",
    },
    "ProcessingJobStatus": "InProgress",  # 'InProgress'|'Completed'|'Failed'|'Stopping'|'Stopped'
    "ExitMessage": "string",
    "FailureReason": "string",
    "ProcessingEndTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "ProcessingStartTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "MonitoringScheduleArn": "string",
    "AutoMLJobArn": auto_ml_job_arn,
    "TrainingJobArn": training_job_arn,
}

transform_job_name = "a-transform-job"
transform_job_arn = (
    "arn:aws:sagemaker:us-west-2:123412341234:transform-job/a-transform-job"
)
list_transform_jobs_response = {
    "TransformJobSummaries": [
        {
            "TransformJobName": transform_job_name,
            "TransformJobArn": transform_job_arn,
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "TransformEndTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "TransformJobStatus": "InProgress",
            "FailureReason": "string",
        },
    ],
}
describe_transform_job_response = {
    "TransformJobName": transform_job_name,
    "TransformJobArn": transform_job_arn,
    "TransformJobStatus": "InProgress",
    # 'InProgress' |'Completed'|'Failed'|'Stopping'|'Stopped'
    "FailureReason": "string",
    "ModelName": "the-second-model",
    "MaxConcurrentTransforms": 123,
    "ModelClientConfig": {
        "InvocationsTimeoutInSeconds": 123,
        "InvocationsMaxRetries": 123,
    },
    "MaxPayloadInMB": 123,
    "BatchStrategy": "MultiRecord",  # 'MultiRecord'|'SingleRecord'
    "Environment": {"string": "string"},
    "TransformInput": {
        "DataSource": {
            "S3DataSource": {
                "S3DataType": "ManifestFile",  # "ManifestFile" | "S3Prefix" | "AugmentedManifestFile"
                "S3Uri": "s3://transform-job/input-data-source.tar.gz",
            }
        },
        "ContentType": "string",
        "CompressionType": "None",  # "None" | "Gzip"
        "SplitType": "None",  # "None" | "Line" | "RecordIO" | "TFRecord"
    },
    "TransformOutput": {
        "S3OutputPath": "s3://transform-job/output.tar.gz",
        "Accept": "string",
        "AssembleWith": "None",  # "None" | "Line"
        "KmsKeyId": "string",
    },
    "TransformResources": {
        "InstanceType": "ml.m4.xlarge",
        "InstanceCount": 123,
        "VolumeKmsKeyId": "string",
    },
    "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "TransformStartTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "TransformEndTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "LabelingJobArn": labeling_job_arn,
    "AutoMLJobArn": auto_ml_job_arn,
    "DataProcessing": {
        "InputFilter": "string",
        "OutputFilter": "string",
        "JoinSource": "Input",  # "Input" | "None"
    },
    "ExperimentConfig": {
        "ExperimentName": "string",
        "TrialName": "string",
        "TrialComponentDisplayName": "string",
    },
}

job_stubs: Mapping[str, Mapping[str, Any]] = {
    "auto_ml": {
        "list": list_auto_ml_jobs_response,
        "describe": describe_auto_ml_job_response,
        "describe_name": auto_ml_job_name,
    },
    "compilation": {
        "list": list_compilation_jobs_response,
        "describe": describe_compilation_job_response,
        "describe_name": compilation_job_name,
    },
    "hyper_parameter_tuning": {
        "list": list_hyper_parameter_tuning_jobs_response,
        "describe": describe_hyper_parameter_tuning_job_response,
        "describe_name": hyper_parameter_tuning_job_name,
    },
    "labeling": {
        "list": list_labeling_jobs_response,
        "describe": describe_labeling_job_response,
        "describe_name": labeling_job_name,
    },
    "processing": {
        "list": list_processing_jobs_response,
        "describe": describe_processing_job_response,
        "describe_name": processing_job_name,
    },
    "training": {
        "list": list_training_jobs_response,
        "describe": describe_training_job_response,
        "describe_name": training_job_name,
    },
    "transform": {
        "list": list_transform_jobs_response,
        "describe": describe_transform_job_response,
        "describe_name": transform_job_name,
    },
}

list_endpoints_response = {
    "Endpoints": [
        {
            "EndpointName": "the-first-endpoint",
            "EndpointArn": "arn:aws:sagemaker:us-west-2:123412341234:endpoint/the-first-endpoint",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "EndpointStatus": "InService",
        },
        {
            "EndpointName": "the-second-endpoint",
            "EndpointArn": "arn:aws:sagemaker:us-west-2:123412341234:endpoint/the-second-endpoint",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "EndpointStatus": "Creating",
        },
    ],
}
describe_endpoint_response_1 = {
    "EndpointName": "the-first-endpoint",
    "EndpointArn": "arn:aws:sagemaker:us-west-2:123412341234:endpoint/the-first-endpoint",
    "EndpointConfigName": "string",
    "ProductionVariants": [
        {
            "VariantName": "string",
            "DeployedImages": [
                {
                    "SpecifiedImage": "string",
                    "ResolvedImage": "string",
                    "ResolutionTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
                },
            ],
            "CurrentWeight": 0.1,
            "DesiredWeight": 0.1,
            "CurrentInstanceCount": 123,
            "DesiredInstanceCount": 123,
        },
    ],
    "DataCaptureConfig": {
        "EnableCapture": True,
        "CaptureStatus": "Started",  # "Started" | "Stopped"
        "CurrentSamplingPercentage": 123,
        "DestinationS3Uri": "string",
        "KmsKeyId": "string",
    },
    "EndpointStatus": "InService",  # 'OutOfService'|'Creating'|'Updating'|'SystemUpdating'|'RollingBack'|'InService'|'Deleting'|'Failed'
    "FailureReason": "string",
    "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "LastDeploymentConfig": {
        "BlueGreenUpdatePolicy": {
            "TrafficRoutingConfiguration": {
                "Type": "ALL_AT_ONCE",  # 'ALL_AT_ONCE'|'CANARY'
                "WaitIntervalInSeconds": 123,
                "CanarySize": {
                    "Type": "INSTANCE_COUNT",  # 'INSTANCE_COUNT'|'CAPACITY_PERCENT'
                    "Value": 123,
                },
            },
            "TerminationWaitInSeconds": 123,
            "MaximumExecutionTimeoutInSeconds": 600,
        },
        "AutoRollbackConfiguration": {
            "Alarms": [
                {"AlarmName": "string"},
            ]
        },
    },
}
describe_endpoint_response_2 = {
    "EndpointName": "the-second-endpoint",
    "EndpointArn": "arn:aws:sagemaker:us-west-2:123412341234:endpoint/the-second-endpoint",
    "EndpointConfigName": "string",
    "ProductionVariants": [
        {
            "VariantName": "string",
            "DeployedImages": [
                {
                    "SpecifiedImage": "string",
                    "ResolvedImage": "string",
                    "ResolutionTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
                },
            ],
            "CurrentWeight": 0.1,
            "DesiredWeight": 0.1,
            "CurrentInstanceCount": 123,
            "DesiredInstanceCount": 123,
        },
    ],
    "DataCaptureConfig": {
        "EnableCapture": True,
        "CaptureStatus": "Started",  # "Started" | "Stopped"
        "CurrentSamplingPercentage": 123,
        "DestinationS3Uri": "string",
        "KmsKeyId": "string",
    },
    "EndpointStatus": "Creating",  # 'OutOfService'|'Creating'|'Updating'|'SystemUpdating'|'RollingBack'|'InService'|'Deleting'|'Failed'
    "FailureReason": "string",
    "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "LastDeploymentConfig": {
        "BlueGreenUpdatePolicy": {
            "TrafficRoutingConfiguration": {
                "Type": "ALL_AT_ONCE",  # 'ALL_AT_ONCE'|'CANARY'
                "WaitIntervalInSeconds": 123,
                "CanarySize": {
                    "Type": "INSTANCE_COUNT",  # 'INSTANCE_COUNT'|'CAPACITY_PERCENT'
                    "Value": 123,
                },
            },
            "TerminationWaitInSeconds": 123,
            "MaximumExecutionTimeoutInSeconds": 600,
        },
        "AutoRollbackConfiguration": {
            "Alarms": [
                {"AlarmName": "string"},
            ]
        },
    },
}

list_models_response = {
    "Models": [
        {
            "ModelName": "the-first-model",
            "ModelArn": "arn:aws:sagemaker:us-west-2:123412341234:model/the-first-model",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        },
        {
            "ModelName": "the-second-model",
            "ModelArn": "arn:aws:sagemaker:us-west-2:123412341234:model/the-second-model",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        },
    ],
}
describe_model_response_1 = {
    "ModelName": "the-first-model",
    "PrimaryContainer": {
        "ContainerHostname": "string",
        "Image": "123412341234.dkr.ecr.us-west-2.amazonaws.com/the-first-model-image",
        "ImageConfig": {
            "RepositoryAccessMode": "Platform",  # 'Platform'|'Vpc'
            "RepositoryAuthConfig": {"RepositoryCredentialsProviderArn": "string"},
        },
        "Mode": "SingleModel",  # 'SingleModel'|'MultiModel'
        "ModelDataUrl": "s3://the-first-model-data-url/data.tar.gz",
        "Environment": {"string": "string"},
        "ModelPackageName": "string",
        "MultiModelConfig": {
            "ModelCacheSetting": "Enabled",  # 'Enabled'|'Disabled'
        },
    },
    "Containers": [
        {
            "ContainerHostname": "string",
            "Image": "string",
            "ImageConfig": {
                "RepositoryAccessMode": "Platform",  # 'Platform'|'Vpc'
                "RepositoryAuthConfig": {"RepositoryCredentialsProviderArn": "string"},
            },
            "Mode": "SingleModel",  # 'SingleModel'|'MultiModel'
            # dangling pointer, no training job corresponding to this yet
            "ModelDataUrl": "s3://training-job-2/model-artifact.tar.gz",
            "Environment": {"string": "string"},
            "ModelPackageName": "string",
            "MultiModelConfig": {
                "ModelCacheSetting": "Enabled",  # 'Enabled'|'Disabled'
            },
        },
    ],
    "InferenceExecutionConfig": {
        "Mode": "Serial",  # 'Serial'|'Direct'
    },
    "ExecutionRoleArn": "arn:aws:iam::123412341234:role/service-role/AmazonSageMaker-ExecutionRole-20210614T104201",
    "VpcConfig": {
        "SecurityGroupIds": [
            "string",
        ],
        "Subnets": [
            "string",
        ],
    },
    "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "ModelArn": "arn:aws:sagemaker:us-west-2:123412341234:model/the-first-model",
    "EnableNetworkIsolation": True,  # True | False
}
describe_model_response_2 = {
    "ModelName": "the-second-model",
    "PrimaryContainer": {
        "ContainerHostname": "string",
        "Image": "123412341234.dkr.ecr.us-west-2.amazonaws.com/the-second-model-image",
        "ImageConfig": {
            "RepositoryAccessMode": "Platform",  # 'Platform'|'Vpc'
            "RepositoryAuthConfig": {"RepositoryCredentialsProviderArn": "string"},
        },
        "Mode": "MultiModel",  # 'SingleModel'|'MultiModel'
        "ModelDataUrl": "s3://the-second-model-data-url/data.tar.gz",
        "Environment": {"string": "string"},
        "ModelPackageName": "string",
        "MultiModelConfig": {
            "ModelCacheSetting": "Disabled",  # 'Enabled'|'Disabled'
        },
    },
    "Containers": [
        {
            "ContainerHostname": "string",
            "Image": "string",
            "ImageConfig": {
                "RepositoryAccessMode": "Vpc",  # 'Platform'|'Vpc'
                "RepositoryAuthConfig": {"RepositoryCredentialsProviderArn": "string"},
            },
            "Mode": "SingleModel",  # 'SingleModel'|'MultiModel'
            "ModelDataUrl": "s3://the-first-model-data-url/data.tar.gz",
            "Environment": {"string": "string"},
            "ModelPackageName": "string",
            "MultiModelConfig": {
                "ModelCacheSetting": "Disabled",  # 'Enabled'|'Disabled'
            },
        },
    ],
    "InferenceExecutionConfig": {
        "Mode": "Serial",  # 'Serial'|'Direct'
    },
    "ExecutionRoleArn": "arn:aws:iam::123412341234:role/service-role/AmazonSageMaker-ExecutionRole-20210614T104201",
    "VpcConfig": {
        "SecurityGroupIds": [
            "string",
        ],
        "Subnets": [
            "string",
        ],
    },
    "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "ModelArn": "arn:aws:sagemaker:us-west-2:123412341234:model/the-second-model",
    "EnableNetworkIsolation": False,  # True | False
}

list_actions_response = {
    "ActionSummaries": [
        {
            "ActionArn": "arn:aws:sagemaker:us-west-2:123412341234:action/deploy-the-first-endpoint",
            "ActionName": "deploy-the-first-endpoint",
            "Source": {
                "SourceUri": "arn:aws:sagemaker:us-west-2:123412341234:endpoint/the-first-endpoint",
                "SourceType": "ARN",
                "SourceId": "1",
            },
            "ActionType": "ModelDeployment",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        },
        {
            "ActionArn": "arn:aws:sagemaker:us-west-2:123412341234:action/deploy-the-second-endpoint",
            "ActionName": "deploy-the-second-endpoint",
            "Source": {
                "SourceUri": "arn:aws:sagemaker:us-west-2:123412341234:endpoint/the-second-endpoint",
                "SourceType": "ARN",
                "SourceId": "1",
            },
            "ActionType": "ModelDeployment",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        },
    ],
}

list_first_endpoint_incoming_response = {
    "AssociationSummaries": [
        {
            "SourceArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-first-model-artifact",
            "DestinationArn": "arn:aws:sagemaker:us-west-2:123412341234:action/deploy-the-first-endpoint",
            "SourceType": "Model",
            "DestinationType": "ModelDeployment",
            "AssociationType": "ContributedTo",
            "DestinationName": "deploy-the-first-endpoint",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "CreatedBy": {},
        },
        {
            "SourceArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-first-model-image",
            "DestinationArn": "arn:aws:sagemaker:us-west-2:123412341234:action/deploy-the-first-endpoint",
            "SourceType": "Image",
            "DestinationType": "ModelDeployment",
            "AssociationType": "ContributedTo",
            "DestinationName": "deploy-the-first-endpoint",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "CreatedBy": {},
        },
    ],
}

list_first_endpoint_outgoing_response = {
    "AssociationSummaries": [
        {
            "SourceArn": "arn:aws:sagemaker:us-west-2:123412341234:action/deploy-the-first-endpoint",
            "DestinationArn": "arn:aws:sagemaker:us-west-2:123412341234:context/the-first-endpoint-context",
            "SourceType": "ModelDeployment",
            "DestinationType": "Endpoint",
            "AssociationType": "AssociatedWith",
            "SourceName": "deploy-the-first-endpoint",
            "DestinationName": "the-first-endpoint-artifact",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "CreatedBy": {},
        }
    ],
}


list_second_endpoint_incoming_response = {
    "AssociationSummaries": [
        {
            "SourceArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-second-model-artifact",
            "DestinationArn": "arn:aws:sagemaker:us-west-2:123412341234:action/deploy-the-second-endpoint",
            "SourceType": "Model",
            "DestinationType": "ModelDeployment",
            "AssociationType": "ContributedTo",
            "DestinationName": "deploy-the-second-endpoint",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "CreatedBy": {},
        },
        {
            "SourceArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-second-model-image",
            "DestinationArn": "arn:aws:sagemaker:us-west-2:123412341234:action/deploy-the-second-endpoint",
            "SourceType": "Image",
            "DestinationType": "ModelDeployment",
            "AssociationType": "ContributedTo",
            "DestinationName": "deploy-the-second-endpoint",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "CreatedBy": {},
        },
    ],
}

list_second_endpoint_outgoing_response = {
    "AssociationSummaries": [
        {
            "SourceArn": "arn:aws:sagemaker:us-west-2:123412341234:action/deploy-the-second-endpoint",
            "DestinationArn": "arn:aws:sagemaker:us-west-2:123412341234:context/the-second-endpoint-context",
            "SourceType": "ModelDeployment",
            "DestinationType": "Endpoint",
            "AssociationType": "AssociatedWith",
            "SourceName": "deploy-the-second-endpoint",
            "DestinationName": "the-second-endpoint-artifact",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "CreatedBy": {},
        }
    ],
}

list_artifacts_response = {
    "ArtifactSummaries": [
        {
            "ArtifactArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-first-model-artifact",
            "Source": {"SourceUri": "s3://the-first-model-data-url/data.tar.gz"},
            "ArtifactType": "Model",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        },
        {
            "ArtifactArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-second-model-artifact",
            "Source": {"SourceUri": "s3://the-second-model-data-url/data.tar.gz"},
            "ArtifactType": "Model",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        },
        {
            "ArtifactArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-first-model-image",
            "Source": {
                "SourceUri": "123412341234.dkr.ecr.us-west-2.amazonaws.com/the-first-model-image",
                "SourceTypes": [],
            },
            "ArtifactType": "Image",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        },
        {
            "ArtifactArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-second-model-image",
            "Source": {
                "SourceUri": "123412341234.dkr.ecr.us-west-2.amazonaws.com/the-second-model-image",
                "SourceTypes": [],
            },
            "ArtifactType": "Image",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        },
    ],
}

list_contexts_response = {
    "ContextSummaries": [
        {
            "ContextArn": "arn:aws:sagemaker:us-west-2:123412341234:context/the-first-endpoint-context",
            "ContextName": "the-first-endpoint-context",
            "Source": {
                "SourceUri": "arn:aws:sagemaker:us-west-2:123412341234:endpoint/the-first-endpoint",
                "SourceType": "ARN",
                "SourceId": "Wed Jul 14 23:26:59 UTC 2021",
            },
            "ContextType": "Endpoint",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        },
        {
            "ContextArn": "arn:aws:sagemaker:us-west-2:123412341234:context/the-second-endpoint-context",
            "ContextName": "the-second-endpoint-context",
            "Source": {
                "SourceUri": "arn:aws:sagemaker:us-west-2:123412341234:endpoint/the-second-endpoint",
                "SourceType": "ARN",
                "SourceId": "Wed Jul 14 23:26:59 UTC 2021",
            },
            "ContextType": "Endpoint",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        },
        {
            "ContextArn": "arn:aws:sagemaker:us-west-2:123412341234:context/a-model-package-group-context",
            "ContextName": "a-model-package-group-context",
            "Source": {
                "SourceUri": "arn:aws:sagemaker:us-west-2:123412341234:model-package-group/a-model-package-group",
                "SourceType": "ARN",
                "SourceId": "Wed Jul 14 23:19:46 UTC 2021",
            },
            "ContextType": "ModelGroup",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "LastModifiedTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
        },
    ]
}

get_model_group_incoming_response = {
    "AssociationSummaries": [
        {
            "SourceArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-first-model-package-artifact",
            "DestinationArn": "arn:aws:sagemaker:us-west-2:123412341234:context/a-model-package-group-context",
            "SourceType": "Model",
            "DestinationType": "ModelGroup",
            "AssociationType": "AssociatedWith",
            "DestinationName": "a-model-package-group-context",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "CreatedBy": {},
        },
        {
            "SourceArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-second-model-package-artifact",
            "DestinationArn": "arn:aws:sagemaker:us-west-2:123412341234:context/a-model-package-group-context",
            "SourceType": "Model",
            "DestinationType": "ModelGroup",
            "AssociationType": "AssociatedWith",
            "DestinationName": "a-model-package-group-context",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "CreatedBy": {},
        },
    ]
}

get_first_model_package_incoming_response = {
    "AssociationSummaries": [
        {
            "SourceArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-first-model-artifact",
            "DestinationArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-first-model-package-artifact",
            "SourceType": "Model",
            "DestinationType": "Model",
            "AssociationType": "ContributedTo",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "CreatedBy": {},
        },
        {
            "SourceArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-first-model-image",
            "DestinationArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-first-model-package-artifact",
            "SourceType": "Image",
            "DestinationType": "Model",
            "AssociationType": "ContributedTo",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "CreatedBy": {},
        },
    ]
}

get_second_model_package_incoming_response = {
    "AssociationSummaries": [
        {
            "SourceArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-second-model-artifact",
            "DestinationArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-second-model-package-artifact",
            "SourceType": "Model",
            "DestinationType": "Model",
            "AssociationType": "ContributedTo",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "CreatedBy": {},
        },
        {
            "SourceArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-second-model-image",
            "DestinationArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-second-model-package-artifact",
            "SourceType": "Image",
            "DestinationType": "Model",
            "AssociationType": "ContributedTo",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "CreatedBy": {},
        },
    ]
}

list_groups_response = {
    "ModelPackageGroupSummaryList": [
        {
            "ModelPackageGroupName": "a-model-package-group",
            "ModelPackageGroupArn": "arn:aws:sagemaker:us-west-2:123412341234:model-package-group/a-model-package-group",
            "ModelPackageGroupDescription": "Just a model package group.",
            "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
            "ModelPackageGroupStatus": "Completed",  # 'Pending'|'InProgress'|'Completed'|'Failed'|'Deleting'|'DeleteFailed'
        },
    ]
}

describe_group_response = {
    "ModelPackageGroupName": "a-model-package-group",
    "ModelPackageGroupArn": "arn:aws:sagemaker:us-west-2:123412341234:model-package-group/a-model-package-group",
    "ModelPackageGroupDescription": "Just a model package group.",
    "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    "CreatedBy": {
        "UserProfileArn": "arn:aws:sagemaker:us-west-2:123412341234:user-profile/some-domain/some-user",
        "UserProfileName": "some-user",
        "DomainId": "some-domain",
    },
    "ModelPackageGroupStatus": "Completed",  # 'Pending'|'InProgress'|'Completed'|'Failed'|'Deleting'|'DeleteFailed'
}
