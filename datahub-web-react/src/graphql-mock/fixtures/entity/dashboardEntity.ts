import * as faker from 'faker';
// import { generatePlatform } from 'generateDataPlatform';
import kafkaLogo from '../../../images/kafkalogo.png';
import s3Logo from '../../../images/s3.png';
import snowflakeLogo from '../../../images/snowflakelogo.png';
import bigqueryLogo from '../../../images/bigquerylogo.png';
import { generateTag } from '../tag';
import { Dashboard, DataPlatform, EntityType, Ownership, OwnershipType, PlatformType } from '../../../types.generated';
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

export const dashboardEntity = (tool): Dashboard => {
    const name = `${faker.company.bsNoun()}`;
    const description = `${faker.commerce.productDescription()}`;
    const datahubUser = findUserByUsername('datahub');
    const kafkaUser = findUserByUsername('kafka');
    const lookerUser = findUserByUsername('looker');
    const datahubOwnership: Ownership = {
        owners: [
            {
                owner: datahubUser,
                type: OwnershipType.Stakeholder,
                __typename: 'Owner',
                associatedUrn: `urn:li:dashboard:(${tool},${name})`,
            },
        ],
        lastModified: { time: 1619993818664, __typename: 'AuditStamp' },
        __typename: 'Ownership',
    };
    const kafkaOwnership: Ownership = {
        owners: [
            {
                owner: kafkaUser,
                type: OwnershipType.Stakeholder,
                __typename: 'Owner',
                associatedUrn: `urn:li:dashboard:(${tool},${name})`,
            },
        ],
        lastModified: { time: 1619993818664, __typename: 'AuditStamp' },
        __typename: 'Ownership',
    };
    const platform = 's3';
    const platformURN = `urn:li:dataPlatform:s3`;
    const dataPlatform = generatePlatform({ platform, urn: platformURN });

    return {
        urn: `urn:li:dashboard:(${tool},${name})`,
        type: EntityType.Dashboard,
        tool,
        dashboardId: '3',
        editableProperties: null,
        info: {
            name,
            description,
            externalUrl: null,
            access: null,
            charts: [],
            lastModified: { time: 1619160920, __typename: 'AuditStamp' },
            created: { time: 1619160920, __typename: 'AuditStamp' },
            __typename: 'DashboardInfo',
        },
        ownership: {
            owners: [
                {
                    owner: datahubUser,
                    type: OwnershipType.Stakeholder,
                    associatedUrn: `urn:li:dashboard:(${tool},${name})`,
                    __typename: 'Owner',
                },
                {
                    owner: kafkaUser,
                    type: OwnershipType.Developer,
                    associatedUrn: `urn:li:dashboard:(${tool},${name})`,
                    __typename: 'Owner',
                },
                {
                    owner: lookerUser,
                    type: OwnershipType.Developer,
                    associatedUrn: `urn:li:dashboard:(${tool},${name})`,
                    __typename: 'Owner',
                },
            ],
            lastModified: { time: 1619993818664, __typename: 'AuditStamp' },
            __typename: 'Ownership',
        },
        globalTags: {
            tags: [
                {
                    tag: generateTag(datahubOwnership),
                    associatedUrn: `urn:li:dashboard:(${tool},${name})`,
                    __typename: 'TagAssociation',
                },
                {
                    tag: generateTag(kafkaOwnership),
                    associatedUrn: `urn:li:dashboard:(${tool},${name})`,
                    __typename: 'TagAssociation',
                },
            ],
            __typename: 'GlobalTags',
        },
        platform: dataPlatform,
        __typename: 'Dashboard',
    };
};
