import kafkaLogo from '../../../images/kafkalogo.png';
import s3Logo from '../../../images/s3.png';
import snowflakeLogo from '../../../images/snowflakelogo.png';
import bigqueryLogo from '../../../images/bigquerylogo.png';
import { DataPlatform, EntityType, PlatformType } from '../../../types.generated';

export const platformLogo = {
    kafka: kafkaLogo,
    s3: s3Logo,
    snowflake: snowflakeLogo,
    bigquery: bigqueryLogo,
};

export const generatePlatform = ({ platform, urn }): DataPlatform => {
    return {
        urn,
        type: EntityType.Dataset,
        name: platform,
        properties: {
            type: PlatformType.Others,
            datasetNameDelimiter: '',
            logoUrl: platformLogo[platform],
            __typename: 'DataPlatformProperties',
        },
        __typename: 'DataPlatform',
    };
};
