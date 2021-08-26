import * as faker from 'faker';
import { generateTag } from '../tag';
import { Dashboard, EntityType, Ownership, OwnershipType } from '../../../types.generated';
import { findUserByUsername } from '../searchResult/userSearchResult';

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
            },
        ],
        lastModified: { time: 1619993818664, __typename: 'AuditStamp' },
        __typename: 'Ownership',
    };

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
                    __typename: 'Owner',
                },
                {
                    owner: kafkaUser,
                    type: OwnershipType.Developer,
                    __typename: 'Owner',
                },
                {
                    owner: lookerUser,
                    type: OwnershipType.Developer,
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
                    __typename: 'TagAssociation',
                },
                {
                    tag: generateTag(kafkaOwnership),
                    __typename: 'TagAssociation',
                },
            ],
            __typename: 'GlobalTags',
        },
        __typename: 'Dashboard',
    };
};
