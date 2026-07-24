import { describe, expect, it } from 'vitest';

import {
    buildDataContractAssertionSearchInput,
    buildUpsertDataContractMutationVariables,
} from '@app/entityV2/shared/tabs/Dataset/Validations/contract/builder/utils';

import { EntityType, FilterOperator } from '@types';

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,quality_demo.orders,PROD)';

describe('data contract builder utils', () => {
    it('builds a bounded assertion search for only the current dataset', () => {
        expect(buildDataContractAssertionSearchInput(DATASET_URN, 25, 25)).toEqual({
            types: [EntityType.Assertion],
            query: '*',
            start: 25,
            count: 25,
            orFilters: [
                {
                    and: [
                        {
                            field: 'entity',
                            values: [DATASET_URN],
                            condition: FilterOperator.Equal,
                        },
                    ],
                },
            ],
            searchFlags: {
                skipCache: true,
            },
        });
    });

    it('preserves selected assertions when building the contract mutation', () => {
        expect(
            buildUpsertDataContractMutationVariables(DATASET_URN, {
                freshness: { assertionUrn: 'urn:li:assertion:freshness' },
                schema: { assertionUrn: 'urn:li:assertion:schema' },
                dataQuality: [
                    { assertionUrn: 'urn:li:assertion:quality-1' },
                    { assertionUrn: 'urn:li:assertion:quality-2' },
                ],
            }),
        ).toEqual({
            input: {
                entityUrn: DATASET_URN,
                freshness: [{ assertionUrn: 'urn:li:assertion:freshness' }],
                schema: [{ assertionUrn: 'urn:li:assertion:schema' }],
                dataQuality: [
                    { assertionUrn: 'urn:li:assertion:quality-1' },
                    { assertionUrn: 'urn:li:assertion:quality-2' },
                ],
            },
        });
    });
});
