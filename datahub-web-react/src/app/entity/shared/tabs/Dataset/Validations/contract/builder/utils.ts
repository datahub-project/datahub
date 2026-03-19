import {
    DataContractBuilderState,
} from '@app/entity/shared/tabs/Dataset/Validations/contract/builder/types';

import { DataContract } from '@types';

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
