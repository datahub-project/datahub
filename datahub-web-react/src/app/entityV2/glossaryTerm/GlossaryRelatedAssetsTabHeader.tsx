import { Pill } from '@src/alchemy-components';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { formatNumber } from '@src/app/shared/formatNumber';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import React from 'react';
import styled from 'styled-components';

const Styled = styled.div`
    display: flex;
    align-items: center;
`;

const TabName = styled.div`
    padding-right: 4px;
`;

function GlossaryRelatedAssetsTabHeader() {
    const { entityData } = useEntityData();

    // To get the number of related assets
    const { data } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [],
                query: '*',
                count: 0,
                orFilters: [
                    {
                        and: [
                            {
                                field: 'glossaryTerms',
                                values: [entityData?.urn || ''],
                            },
                        ],
                    },
                ],
                searchFlags: {
                    skipCache: true,
                },
            },
        },
        skip: !entityData?.urn,
        fetchPolicy: 'cache-and-network',
    });

    return (
        <Styled>
            <TabName>Related Assets</TabName>
            <Pill label={formatNumber(data?.searchAcrossEntities?.total || 0)} />
        </Styled>
    );
}

export default GlossaryRelatedAssetsTabHeader;
