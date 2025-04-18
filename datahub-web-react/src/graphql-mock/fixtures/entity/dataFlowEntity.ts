import * as faker from 'faker';
// import { generatePlatform } from 'generateDataPlatform';
import kafkaLogo from '../../../images/kafkalogo.png';
import s3Logo from '../../../images/s3.png';
import snowflakeLogo from '../../../images/snowflakelogo.png';
import bigqueryLogo from '../../../images/bigquerylogo.png';
import { DataFlow, DataPlatform, EntityType, OwnershipType, PlatformType } from '../../../types.generated';
import { findUserByUsername } from '../searchResult/userSearchResult';

export type DataFlowEntityArg = {
    orchestrator: string;
    cluster: string;
};

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

export const dataFlowEntity = ({ orchestrator, cluster }: DataFlowEntityArg): DataFlow => {
    const flowId = `${faker.company.bsNoun()}_${faker.company.bsNoun()}`;
    const description = `${faker.commerce.productDescription()}`;
    const datahubUser = findUserByUsername('datahub');
    const platform = 'kafka';
    const platformURN = `urn:li:dataPlatform:kafka`;
    const dataPlatform = generatePlatform({ platform, urn: platformURN });

    return {
        urn: `urn:li:dataFlow:(${orchestrator},${flowId},${cluster})`,
        type: EntityType.DataFlow,
        orchestrator,
        flowId,
        cluster,
        info: {
            name: flowId,
            description,
            project: null,
            __typename: 'DataFlowInfo',
        },
        editableProperties: null,
        ownership: {
            owners: [
                {
                    owner: datahubUser,
                    type: OwnershipType.Stakeholder,
                    associatedUrn: `urn:li:dataFlow:(${orchestrator},${flowId},${cluster})`,
                    __typename: 'Owner',
                },
            ],
            lastModified: { time: 1620224528712, __typename: 'AuditStamp' },
            __typename: 'Ownership',
        },
        globalTags: { tags: [], __typename: 'GlobalTags' },
        dataJobs: null,
        platform: dataPlatform,
        __typename: 'DataFlow',
    };
};
