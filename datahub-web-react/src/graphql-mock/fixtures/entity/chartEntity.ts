import * as faker from 'faker';
import { Chart, ChartType, EntityType, OwnershipType } from '../../../types.generated';
import { findUserByUsername } from '../searchResult/userSearchResult';

export const chartEntity = (tool): Chart => {
    const name = `${faker.company.bsNoun()}_${faker.company.bsNoun()}_${faker.company.bsNoun()}`;
    const description = `${faker.commerce.productDescription()}`;
    const datahubUser = findUserByUsername('datahub');

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
                },
            ],
            lastModified: { time: 1619717962718, __typename: 'AuditStamp' },
            __typename: 'Ownership',
        },
        globalTags: null,
        __typename: 'Chart',
    };
};
