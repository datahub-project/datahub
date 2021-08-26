import * as faker from 'faker';
import { DataFlow, EntityType, OwnershipType } from '../../../types.generated';
import { findUserByUsername } from '../searchResult/userSearchResult';

export type DataFlowEntityArg = {
    orchestrator: string;
    cluster: string;
};

export const dataFlowEntity = ({ orchestrator, cluster }: DataFlowEntityArg): DataFlow => {
    const flowId = `${faker.company.bsNoun()}_${faker.company.bsNoun()}`;
    const description = `${faker.commerce.productDescription()}`;
    const datahubUser = findUserByUsername('datahub');

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
                    __typename: 'Owner',
                },
            ],
            lastModified: { time: 1620224528712, __typename: 'AuditStamp' },
            __typename: 'Ownership',
        },
        globalTags: { tags: [], __typename: 'GlobalTags' },
        dataJobs: null,
        __typename: 'DataFlow',
    };
};
