import datetime

list_feature_groups_response = {
    "FeatureGroupSummaries": [
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
