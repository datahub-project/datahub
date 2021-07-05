import datetime

list_feature_groups_response = {
    "FeatureGroupSummaries": [
        {
            "FeatureGroupName": "test-2",
            "FeatureGroupArn": "arn:aws:sagemaker:us-west-2:123412341234:feature-group/test-2",
            "CreationTime": datetime.datetime(2021, 6, 24, 9, 48, 37, 35000),
            "FeatureGroupStatus": "Created",
        },
        {
            "FeatureGroupName": "test-1",
            "FeatureGroupArn": "arn:aws:sagemaker:us-west-2:123412341234:feature-group/test-1",
            "CreationTime": datetime.datetime(2021, 6, 23, 13, 58, 10, 264000),
            "FeatureGroupStatus": "Created",
        },
        {
            "FeatureGroupName": "test",
            "FeatureGroupArn": "arn:aws:sagemaker:us-west-2:123412341234:feature-group/test",
            "CreationTime": datetime.datetime(2021, 6, 14, 11, 3, 0, 803000),
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
    "CreationTime": datetime.datetime(2021, 6, 24, 9, 48, 37, 35000),
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
    "CreationTime": datetime.datetime(2021, 6, 23, 13, 58, 10, 264000),
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
    "CreationTime": datetime.datetime(
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
