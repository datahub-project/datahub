import { DataContract } from '../../../../../../../../types.generated';
import { DataContractBuilderState, DataContractCategoryType } from './types';

/**
 * Creates a builder state instance from a Data Contract object.
 */
export const createBuilderState = (contract?: DataContract | null): DataContractBuilderState | undefined => {
    if (contract) {
        return {
            schema:
                (contract?.properties?.schema?.length && {
                    assertionUrn: contract?.properties?.schema[0]?.assertion?.urn,
                }) ||
                undefined,
            freshness:
                (contract?.properties?.freshness?.length && {
                    assertionUrn: contract?.properties?.freshness[0]?.assertion?.urn,
                }) ||
                undefined,
            dataQuality:
                contract?.properties?.dataQuality?.map((c) => ({ assertionUrn: c.assertion.urn })) || undefined,
        };
    }
    return undefined;
};

/**
 * Constructs the input variables required for upserting a data contract using graphql
 */
export const buildUpsertDataContractMutationVariables = (entityUrn: string, state: DataContractBuilderState) => {
    return {
        input: {
            entityUrn,
            freshness: (state.freshness && [state.freshness]) || [],
            schema: (state.schema && [state.schema]) || [],
            dataQuality: state.dataQuality || [],
        },
    };
};

/**
 * Constructs the input variables required for removing an assertion from a data contract using graphql.
 */
export const buildRemoveAssertionFromContractMutationVariables = (
    entityUrn: string,
    assertionUrn: string,
    contract?: DataContract,
) => {
    return {
        input: {
            entityUrn,
            freshness: contract?.properties?.freshness
                ?.filter((c) => c.assertion.urn !== assertionUrn)
                ?.map((c) => ({
                    assertionUrn: c.assertion.urn,
                })),
            schema: contract?.properties?.schema
                ?.filter((c) => c.assertion.urn !== assertionUrn)
                ?.map((c) => ({
                    assertionUrn: c.assertion.urn,
                })),
            dataQuality: contract?.properties?.dataQuality
                ?.filter((c) => c.assertion.urn !== assertionUrn)
                ?.map((c) => ({
                    assertionUrn: c.assertion.urn,
                })),
        },
    };
};

/**
 * Constructs the input variables required for adding an assertion to a data contract using graphql.
 */
export const buildAddAssertionToContractMutationVariables = (
    category: DataContractCategoryType,
    entityUrn: string,
    assertionUrn: string,
    contract?: DataContract,
) => {
    const baseInput = {
        entityUrn,
        freshness: contract?.properties?.freshness?.map((c) => ({
            assertionUrn: c.assertion.urn,
        })),
        schema: contract?.properties?.schema?.map((c) => ({
            assertionUrn: c.assertion.urn,
        })),
        dataQuality: contract?.properties?.dataQuality?.map((c) => ({
            assertionUrn: c.assertion.urn,
        })),
    };

    switch (category) {
        case DataContractCategoryType.SCHEMA:
            // Replace schema assertion. We only support 1 schema assertion at a time (currently).
            return {
                input: {
                    ...baseInput,
                    schema: [{ assertionUrn }],
                },
            };
        case DataContractCategoryType.FRESHNESS:
            // Replace freshness assertion. We only support 1 freshness assertion at a time (currently).
            return {
                input: {
                    ...baseInput,
                    freshness: [{ assertionUrn }],
                },
            };
        case DataContractCategoryType.DATA_QUALITY:
            return {
                input: {
                    ...baseInput,
                    dataQuality: [...(baseInput.dataQuality || []), { assertionUrn }],
                },
            };
        default:
            throw new Error(`Unrecognized category type ${category} provided.`);
    }
};
