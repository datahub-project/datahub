from datetime import datetime

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

describe_auto_ml_job_response = {
    "AutoMLJobName": "string",
    "AutoMLJobArn": "string",
    "InputDataConfig": [
        {
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "ManifestFile",  # 'ManifestFile'|'S3Prefix'
                    "S3Uri": "string",
                }
            },
            "CompressionType": "None",  #  'None'|'Gzip'
            "TargetAttributeName": "string",
        },
    ],
    "OutputDataConfig": {"KmsKeyId": "string", "S3OutputPath": "string"},
    "RoleArn": "string",
    "AutoMLJobObjective": {
        "MetricName": "Accuracy",  # 'Accuracy'|'MSE'|'F1'|'F1macro'|'AUC'
    },
    "ProblemType": "BinaryClassification",  # 'BinaryClassification'|'MulticlassClassification'|'Regression'
    "AutoMLJobConfig": {
        "CompletionCriteria": {
            "MaxCandidates": 123,
            "MaxRuntimePerTrainingJobInSeconds": 123,
            "MaxAutoMLJobRuntimeInSeconds": 123,
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
    },
    "CreationTime": datetime(2015, 1, 1),
    "EndTime": datetime(2015, 1, 1),
    "LastModifiedTime": datetime(2015, 1, 1),
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
                "ModelDataUrl": "string",
                "Environment": {"string": "string"},
            },
        ],
        "CreationTime": datetime(2015, 1, 1),
        "EndTime": datetime(2015, 1, 1),
        "LastModifiedTime": datetime(2015, 1, 1),
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
    "GenerateCandidateDefinitionsOnly": True,  # True | False
    "AutoMLJobArtifacts": {
        "CandidateDefinitionNotebookLocation": "string",
        "DataExplorationNotebookLocation": "string",
    },
    "ResolvedAttributes": {
        "AutoMLJobObjective": {
            "MetricName": "Accuracy",  # "Accuracy" | "MSE" | "F1" | "F1macro" | "AUC"
        },
        "ProblemType": "BinaryClassification",
        # "BinaryClassification"
        # | "MulticlassClassification"
        # | "Regression",
        "CompletionCriteria": {
            "MaxCandidates": 123,
            "MaxRuntimePerTrainingJobInSeconds": 123,
            "MaxAutoMLJobRuntimeInSeconds": 123,
        },
    },
    "ModelDeployConfig": {
        "AutoGenerateEndpointName": True,  # True | False
        "EndpointName": "string",
    },
    "ModelDeployResult": {"EndpointName": "string"},
}

describe_compilation_job_response = {
    "CompilationJobName": "string",
    "CompilationJobArn": "string",
    "CompilationJobStatus": "INPROGRESS",  # 'INPROGRESS'|'COMPLETED'|'FAILED'|'STARTING'|'STOPPING'|'STOPPED'
    "CompilationStartTime": datetime(2015, 1, 1),
    "CompilationEndTime": datetime(2015, 1, 1),
    "StoppingCondition": {"MaxRuntimeInSeconds": 123, "MaxWaitTimeInSeconds": 123},
    "InferenceImage": "string",
    "CreationTime": datetime(2015, 1, 1),
    "LastModifiedTime": datetime(2015, 1, 1),
    "FailureReason": "string",
    "ModelArtifacts": {"S3ModelArtifacts": "string"},
    "ModelDigests": {"ArtifactDigest": "string"},
    "RoleArn": "string",
    "InputConfig": {
        "S3Uri": "string",
        "DataInputConfig": "string",
        "Framework": "TENSORFLOW",  # 'TENSORFLOW'|'KERAS'|'MXNET'|'ONNX'|'PYTORCH'|'XGBOOST'|'TFLITE'|'DARKNET'|'SKLEARN'
        "FrameworkVersion": "string",
    },
    "OutputConfig": {
        "S3OutputLocation": "string",
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

describe_edge_packaging_job_response = {
    "EdgePackagingJobArn": "string",
    "EdgePackagingJobName": "string",
    "CompilationJobName": "string",
    "ModelName": "string",
    "ModelVersion": "string",
    "RoleArn": "string",
    "OutputConfig": {
        "S3OutputLocation": "string",
        "KmsKeyId": "string",
        "PresetDeploymentType": "GreengrassV2Component",
        "PresetDeploymentConfig": "string",
    },
    "ResourceKey": "string",
    "EdgePackagingJobStatus": "STARTING",  # 'STARTING'|'INPROGRESS'|'COMPLETED'|'FAILED'|'STOPPING'|'STOPPED'
    "EdgePackagingJobStatusMessage": "string",
    "CreationTime": datetime(2015, 1, 1),
    "LastModifiedTime": datetime(2015, 1, 1),
    "ModelArtifact": "string",
    "ModelSignature": "string",
    "PresetDeploymentOutput": {
        "Type": "GreengrassV2Component",
        "Artifact": "string",
        "Status": "COMPLETED",  # 'COMPLETED'|'FAILED'
        "StatusMessage": "string",
    },
}


describe_hyper_parameter_tuning_job_response = {
    "HyperParameterTuningJobName": "string",
    "HyperParameterTuningJobArn": "string",
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
        "TuningJobCompletionCriteria": {"TargetObjectiveMetricValue": ...},
    },
    "TrainingJobDefinition": {
        "DefinitionName": "string",
        "TuningObjective": {"Type": "Maximize" | "Minimize", "MetricName": "string"},
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
        "RoleArn": "string",
        "InputDataConfig": [
            {
                "ChannelName": "string",
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "ManifestFile",  # 'ManifestFile'|'S3Prefix'|'AugmentedManifestFile'
                        "S3Uri": "string",
                        "S3DataDistributionType": "FullyReplicated",  # 'FullyReplicated'|'ShardedByS3Key'
                        "AttributeNames": [
                            "string",
                        ],
                    },
                    "FileSystemDataSource": {
                        "FileSystemId": "string",
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
        "OutputDataConfig": {"KmsKeyId": "string", "S3OutputPath": "string"},
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
        "CheckpointConfig": {"S3Uri": "string", "LocalPath": "string"},
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
            "RoleArn": "string",
            "InputDataConfig": [
                {
                    "ChannelName": "string",
                    "DataSource": {
                        "S3DataSource": {
                            "S3DataType": "ManifestFile",  # 'ManifestFile'|'S3Prefix'|'AugmentedManifestFile'
                            "S3Uri": "string",
                            "S3DataDistributionType": "FullyReplicated",  # 'FullyReplicated'|'ShardedByS3Key'
                            "AttributeNames": [
                                "string",
                            ],
                        },
                        "FileSystemDataSource": {
                            "FileSystemId": "string",
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
            "OutputDataConfig": {"KmsKeyId": "string", "S3OutputPath": "string"},
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
            "CheckpointConfig": {"S3Uri": "string", "LocalPath": "string"},
            "RetryStrategy": {"MaximumRetryAttempts": 123},
        },
    ],
    "HyperParameterTuningJobStatus": "Completed",  # 'Completed'|'InProgress'|'Failed'|'Stopped'|'Stopping'
    "CreationTime": datetime(2015, 1, 1),
    "HyperParameterTuningEndTime": datetime(2015, 1, 1),
    "LastModifiedTime": datetime(2015, 1, 1),
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
        "CreationTime": datetime(2015, 1, 1),
        "TrainingStartTime": datetime(2015, 1, 1),
        "TrainingEndTime": datetime(2015, 1, 1),
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
        "CreationTime": datetime(2015, 1, 1),
        "TrainingStartTime": datetime(2015, 1, 1),
        "TrainingEndTime": datetime(2015, 1, 1),
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
    "CreationTime": datetime(2015, 1, 1),
    "LastModifiedTime": datetime(2015, 1, 1),
    "JobReferenceCode": "string",
    "LabelingJobName": "string",
    "LabelingJobArn": "string",
    "LabelAttributeName": "string",
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
    "OutputConfig": {
        "S3OutputPath": "string",
        "KmsKeyId": "string",
        "SnsTopicArn": "string",
    },
    "RoleArn": "string",
    "LabelCategoryConfigS3Uri": "string",
    "StoppingConditions": {
        "MaxHumanLabeledObjectCount": 123,
        "MaxPercentageOfInputDatasetLabeled": 123,
    },
    "LabelingJobAlgorithmsConfig": {
        "LabelingJobAlgorithmSpecificationArn": "string",
        "InitialActiveLearningModelArn": "string",
        "LabelingJobResourceConfig": {"VolumeKmsKeyId": "string"},
    },
    "HumanTaskConfig": {
        "WorkteamArn": "string",
        "UiConfig": {"UiTemplateS3Uri": "string", "HumanTaskUiArn": "string"},
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
        "OutputDatasetS3Uri": "string",
        "FinalActiveLearningModelArn": "string",
    },
}

describe_processing_job_response = {
    "ProcessingInputs": [
        {
            "InputName": "string",
            "AppManaged": True,  # True|False
            "S3Input": {
                "S3Uri": "string",
                "LocalPath": "string",
                "S3DataType": "ManifestFile",  # 'ManifestFile'|'S3Prefix'
                "S3InputMode": "Pipe",  # 'Pipe'|'File'
                "S3DataDistributionType": "FullyReplicated",  # 'FullyReplicated'|'ShardedByS3Key'
                "S3CompressionType": "None",  # 'None'|'Gzip'
            },
            "DatasetDefinition": {
                "AthenaDatasetDefinition": {
                    "Catalog": "string",
                    "Database": "string",
                    "QueryString": "string",
                    "WorkGroup": "string",
                    "OutputS3Uri": "string",
                    "KmsKeyId": "string",
                    "OutputFormat": "PARQUET",  # 'PARQUET'|'ORC'|'AVRO'|'JSON'|'TEXTFILE'
                    "OutputCompression": "GZIP",  # 'GZIP'|'SNAPPY'|'ZLIB'
                },
                "RedshiftDatasetDefinition": {
                    "ClusterId": "string",
                    "Database": "string",
                    "DbUser": "string",
                    "QueryString": "string",
                    "ClusterRoleArn": "string",
                    "OutputS3Uri": "string",
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
                    "S3Uri": "string",
                    "LocalPath": "string",
                    "S3UploadMode": "Continuous",  # 'Continuous'|'EndOfJob'
                },
                "FeatureStoreOutput": {"FeatureGroupName": "string"},
                "AppManaged": True,  # True|False
            },
        ],
        "KmsKeyId": "string",
    },
    "ProcessingJobName": "string",
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
    "RoleArn": "string",
    "ExperimentConfig": {
        "ExperimentName": "string",
        "TrialName": "string",
        "TrialComponentDisplayName": "string",
    },
    "ProcessingJobArn": "string",
    "ProcessingJobStatus": "InProgress",  # 'InProgress'|'Completed'|'Failed'|'Stopping'|'Stopped'
    "ExitMessage": "string",
    "FailureReason": "string",
    "ProcessingEndTime": datetime(2015, 1, 1),
    "ProcessingStartTime": datetime(2015, 1, 1),
    "LastModifiedTime": datetime(2015, 1, 1),
    "CreationTime": datetime(2015, 1, 1),
    "MonitoringScheduleArn": "string",
    "AutoMLJobArn": "string",
    "TrainingJobArn": "string",
}

describe_training_job_response = {
    "TrainingJobName": "string",
    "TrainingJobArn": "string",
    "TuningJobArn": "string",
    "LabelingJobArn": "string",
    "AutoMLJobArn": "string",
    "ModelArtifacts": {"S3ModelArtifacts": "string"},
    "TrainingJobStatus": "InProgress",  # 'InProgress'|'Completed'|'Failed'|'Stopping'|'Stopped'
    "SecondaryStatus": "Starting",  # 'Starting'|'LaunchingMLInstances'|'PreparingTrainingStack'|'Downloading'|'DownloadingTrainingImage'|'Training'|'Uploading'|'Stopping'|'Stopped'|'MaxRuntimeExceeded'|'Completed'|'Failed'|'Interrupted'|'MaxWaitTimeExceeded'|'Updating'|'Restarting'
    "FailureReason": "string",
    "HyperParameters": {"string": "string"},
    "AlgorithmSpecification": {
        "TrainingImage": "string",
        "AlgorithmName": "string",
        "TrainingInputMode": "Pipe",  # 'Pipe'|'File'
        "MetricDefinitions": [
            {"Name": "string", "Regex": "string"},
        ],
        "EnableSageMakerMetricsTimeSeries": True,  # True|False
    },
    "RoleArn": "string",
    "InputDataConfig": [
        {
            "ChannelName": "string",
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "ManifestFile",  # 'ManifestFile'|'S3Prefix'|'AugmentedManifestFile'
                    "S3Uri": "string",
                    "S3DataDistributionType": "FullyReplicated",  #'FullyReplicated'|'ShardedByS3Key'
                    "AttributeNames": [
                        "string",
                    ],
                },
                "FileSystemDataSource": {
                    "FileSystemId": "string",
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
    "OutputDataConfig": {"KmsKeyId": "string", "S3OutputPath": "string"},
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
    "CreationTime": datetime(2015, 1, 1),
    "TrainingStartTime": datetime(2015, 1, 1),
    "TrainingEndTime": datetime(2015, 1, 1),
    "LastModifiedTime": datetime(2015, 1, 1),
    "SecondaryStatusTransitions": [
        {
            "Status": "Starting",  # 'Starting'|'LaunchingMLInstances'|'PreparingTrainingStack'|'Downloading'|'DownloadingTrainingImage'|'Training'|'Uploading'|'Stopping'|'Stopped'|'MaxRuntimeExceeded'|'Completed'|'Failed'|'Interrupted'|'MaxWaitTimeExceeded'|'Updating'|'Restarting'
            "StartTime": datetime(2015, 1, 1),
            "EndTime": datetime(2015, 1, 1),
            "StatusMessage": "string",
        },
    ],
    "FinalMetricDataList": [
        {"MetricName": "string", "Value": 1.0, "Timestamp": datetime(2015, 1, 1)},
    ],
    "EnableNetworkIsolation": True,  # True|False
    "EnableInterContainerTrafficEncryption": True,  # True|False
    "EnableManagedSpotTraining": True,  # True|False
    "CheckpointConfig": {"S3Uri": "string", "LocalPath": "string"},
    "TrainingTimeInSeconds": 123,
    "BillableTimeInSeconds": 123,
    "DebugHookConfig": {
        "LocalPath": "string",
        "S3OutputPath": "string",
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
            "S3OutputPath": "string",
            "RuleEvaluatorImage": "string",
            "InstanceType": "ml.t3.medium",
            "VolumeSizeInGB": 123,
            "RuleParameters": {"string": "string"},
        },
    ],
    "TensorBoardOutputConfig": {"LocalPath": "string", "S3OutputPath": "string"},
    "DebugRuleEvaluationStatuses": [
        {
            "RuleConfigurationName": "string",
            "RuleEvaluationJobArn": "string",
            "RuleEvaluationStatus": "InProgress",  # 'InProgress'|'NoIssuesFound'|'IssuesFound'|'Error'|'Stopping'|'Stopped'
            "StatusDetails": "string",
            "LastModifiedTime": datetime(2015, 1, 1),
        },
    ],
    "ProfilerConfig": {
        "S3OutputPath": "string",
        "ProfilingIntervalInMilliseconds": 123,
        "ProfilingParameters": {"string": "string"},
    },
    "ProfilerRuleConfigurations": [
        {
            "RuleConfigurationName": "string",
            "LocalPath": "string",
            "S3OutputPath": "string",
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
            "LastModifiedTime": datetime(2015, 1, 1),
        },
    ],
    "ProfilingStatus": "Enabled",  # 'Enabled'|'Disabled'
    "RetryStrategy": {"MaximumRetryAttempts": 123},
    "Environment": {"string": "string"},
}

describe_transform_job_response = {
    "TransformJobName": "string",
    "TransformJobArn": "string",
    "TransformJobStatus": "InProgress",
    # 'InProgress' |'Completed'|'Failed'|'Stopping'|'Stopped'
    "FailureReason": "string",
    "ModelName": "string",
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
                "S3Uri": "string",
            }
        },
        "ContentType": "string",
        "CompressionType": "None",  # "None" | "Gzip"
        "SplitType": "None",  # "None" | "Line" | "RecordIO" | "TFRecord"
    },
    "TransformOutput": {
        "S3OutputPath": "string",
        "Accept": "string",
        "AssembleWith": "None",  # "None" | "Line"
        "KmsKeyId": "string",
    },
    "TransformResources": {
        "InstanceType": "ml.m4.xlarge",
        "InstanceCount": 123,
        "VolumeKmsKeyId": "string",
    },
    "CreationTime": datetime(2015, 1, 1),
    "TransformStartTime": datetime(2015, 1, 1),
    "TransformEndTime": datetime(2015, 1, 1),
    "LabelingJobArn": "string",
    "AutoMLJobArn": "string",
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
