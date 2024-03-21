import React, { useState } from 'react';
import { useGetSearchResultsForMultipleQuery } from '../../../../graphql/search.generated';
import { SearchAcrossEntitiesInput } from '../../../../types.generated';
import { useEntityData } from '../../shared/EntityContext';
import { UnionType } from '../../../search/utils/constants';
import { generateOrFilters } from '../../../search/utils/generateOrFilters';

export const RelatedEntitiesTitle = () => {
    const [relatedEntitiesCount, setRelatedEntitiesCount] = useState<number | undefined>(0);

    const { entityData } = useEntityData();

    const entityUrn = entityData?.urn;

    const fixedOrFilters =
        (entityUrn && [
            {
                field: 'glossaryTerms',
                values: [entityUrn],
            },
            {
                field: 'fieldGlossaryTerms',
                values: [entityUrn],
            },
        ]) ||
        [];
    entityData?.isAChildren?.relationships.forEach((term) => {
        const childUrn = term.entity?.urn;

        if (childUrn) {
            fixedOrFilters.push({
                field: 'glossaryTerms',
                values: [childUrn],
            });

            fixedOrFilters.push({
                field: 'fieldGlossaryTerms',
                values: [childUrn],
            });
        }
    });

    const finalFilters = generateOrFilters(UnionType.OR, fixedOrFilters);

    const searchInput: SearchAcrossEntitiesInput = {
        types: [],
        query: '*',
        start: 0,
        count: 10,
        orFilters: finalFilters,
    };

    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: searchInput,
        },
        fetchPolicy: 'cache-first',
        onCompleted: () => {
            setRelatedEntitiesCount(data?.searchAcrossEntities?.total);
        },
        skip: finalFilters?.length === 0,
    });

    return (
        <>
            {!loading && relatedEntitiesCount && relatedEntitiesCount > 0
                ? `Related Entities (${relatedEntitiesCount})`
                : 'Related Entities'}
        </>
    );
};
