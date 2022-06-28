import * as faker from 'faker';
// import { generatePlatform } from 'generateDataPlatform';
import kafkaLogo from '../../../images/kafkalogo.png';
import s3Logo from '../../../images/s3.png';
import snowflakeLogo from '../../../images/snowflakelogo.png';
import bigqueryLogo from '../../../images/bigquerylogo.png';
import { DataPlatform, Dataset, EntityType, FabricType, OwnershipType, PlatformType } from '../../../types.generated';
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

export type DatasetEntityArg = {
    platform: string;
    origin: FabricType;
    path: string;
};

export const datasetEntity = ({
    platform,
    origin,
    path,
}: DatasetEntityArg): Dataset & { previousSchemaMetadata: any } => {
    const name = `${path}.${faker.company.bsNoun()}_${faker.company.bsNoun()}`;
    const description = `${faker.commerce.productDescription()}`;
    const datahubUser = findUserByUsername('datahub');
    const platformURN = `urn:li:dataPlatform:${platform}`;

    return {
        urn: `urn:li:dataset:(${platformURN},${name},${origin.toUpperCase()})`,
        type: EntityType.Dataset,
        name,
        origin,
        description,
        uri: null,
        platform: generatePlatform({ platform, urn: platformURN }),
        platformNativeType: null,
        properties: null,
        editableProperties: null,
        editableSchemaMetadata: null,
        deprecation: null,
        ownership: {
            owners: [
                {
                    owner: datahubUser,
                    type: OwnershipType.Dataowner,
                    __typename: 'Owner',
                },
            ],
            lastModified: {
                time: 1616107219521,
                __typename: 'AuditStamp',
            },
            __typename: 'Ownership',
        },
        globalTags: {
            tags: [],
            __typename: 'GlobalTags',
        },
        institutionalMemory: null,
        usageStats: null,
        glossaryTerms: null,
        schemaMetadata: null,
        previousSchemaMetadata: null,
        __typename: 'Dataset',
        subTypes: null,
        health: [],
    };
};
