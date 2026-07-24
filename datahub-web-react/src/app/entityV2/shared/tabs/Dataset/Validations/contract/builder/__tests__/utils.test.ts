import { describe, expect, it } from 'vitest';

import {
    buildUpsertDataContractMutationVariables,
    partitionAssertionsByContractCategory,
} from '@app/entityV2/shared/tabs/Dataset/Validations/contract/builder/utils';

import { Assertion, AssertionType } from '@types';

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,quality_demo.orders,PROD)';

describe('data contract builder utils', () => {
    it('categorizes custom assertions from their root assertion type', () => {
        const assertions = [
            {
                urn: 'urn:li:assertion:custom',
                info: {
                    type: AssertionType.Custom,
                    customAssertion: { type: 'Volume Check' },
                },
            },
            {
                urn: 'urn:li:assertion:freshness',
                info: { type: AssertionType.Freshness },
            },
            {
                urn: 'urn:li:assertion:schema',
                info: { type: AssertionType.DataSchema },
            },
        ] as unknown as Assertion[];

        const result = partitionAssertionsByContractCategory(assertions);

        expect(result.dataQualityAssertions.map((assertion) => assertion.urn)).toEqual(['urn:li:assertion:custom']);
        expect(result.freshnessAssertions.map((assertion) => assertion.urn)).toEqual(['urn:li:assertion:freshness']);
        expect(result.schemaAssertions.map((assertion) => assertion.urn)).toEqual(['urn:li:assertion:schema']);
    });

    it('preserves selected assertions when building the contract mutation', () => {
        expect(
            buildUpsertDataContractMutationVariables(DATASET_URN, {
                freshness: { assertionUrn: 'urn:li:assertion:freshness' },
                schema: { assertionUrn: 'urn:li:assertion:schema' },
                dataQuality: [{ assertionUrn: 'urn:li:assertion:custom' }],
            }),
        ).toEqual({
            input: {
                entityUrn: DATASET_URN,
                freshness: [{ assertionUrn: 'urn:li:assertion:freshness' }],
                schema: [{ assertionUrn: 'urn:li:assertion:schema' }],
                dataQuality: [{ assertionUrn: 'urn:li:assertion:custom' }],
            },
        });
    });
});
