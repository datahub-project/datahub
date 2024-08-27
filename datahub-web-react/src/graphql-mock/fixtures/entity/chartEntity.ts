import * as faker from 'faker';
// import { generatePlatform } from 'generateDataPlatform';
import { Chart, ChartType, DataPlatform, EntityType, OwnershipType, PlatformType } from '../../../types.generated';
import kafkaLogo from '../../../images/kafkalogo.png';
import s3Logo from '../../../images/s3.png';
import snowflakeLogo from '../../../images/snowflakelogo.png';
import bigqueryLogo from '../../../images/bigquerylogo.png';
import { findUserByUsername } from '../searchResult/userSearchResult';

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

export const chartEntity = (tool): Chart => {
    const name = `${faker.company.bsNoun()}_${faker.company.bsNoun()}_${faker.company.bsNoun()}`;
    const description = `${faker.commerce.productDescription()}`;
    const datahubUser = findUserByUsername('datahub');
    const platform = 'snowflake';
    const platformURN = `urn:li:dataPlatform:snowflake`;
    const dataPlatform = generatePlatform({ platform, urn: platformURN });

    return {
        urn: `urn:li:chart:(${tool},${name})`,
        type: EntityType.Chart,
        tool,
        chartId: '2',
        info: {
            name,
            description,
            externalUrl:
                'https://superset.demo.datahubproject.io/superset/explore/?form_data=%7B%22slice_id%22%3A%202%7D',
            type: ChartType.Pie,
            access: null,
            lastModified: { time: 1619137330, __typename: 'AuditStamp' },
            created: { time: 1619137330, __typename: 'AuditStamp' },
            __typename: 'ChartInfo',
        },
        editableProperties: null,
        ownership: {
            owners: [
                {
                    owner: datahubUser,
                    type: OwnershipType.Stakeholder,
                    __typename: 'Owner',
                    associatedUrn: `urn:li:chart:(${tool},${name})`,
                },
            ],
            lastModified: { time: 1619717962718, __typename: 'AuditStamp' },
            __typename: 'Ownership',
        },
        globalTags: null,
        platform: dataPlatform,
        __typename: 'Chart',
    };
};
