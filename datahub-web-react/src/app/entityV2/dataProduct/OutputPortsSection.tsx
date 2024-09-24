import React, { useEffect, useState } from 'react';
import RefreshIcon from '@mui/icons-material/Refresh';
import OutputIcon from '@mui/icons-material/Output';
import styled from 'styled-components';
import { useListDataProductAssetsLazyQuery, useListDataProductAssetsQuery } from '../../../graphql/search.generated';
import { useEntityData } from '../../entity/shared/EntityContext';
import { SummaryTabHeaderTitle } from '../shared/summary/HeaderComponents';
import { HorizontalList } from '../shared/summary/ListComponents';
import { SCREEN_WIDTH_BREAK_POINT } from './constants';
import { Card } from '../../sharedV2/cards/components';
// import AddOutputPortCard from './AddOutputPortCard';
import { StyledHeaderWrapper } from './AssetsSections';
import { SearchResult } from '../../../types.generated';
import { ANTD_GRAY } from '../shared/constants';
import { OUTPUT_PORTS_FIELD } from '../../search/utils/constants';
import SummaryEntityCard from '../../sharedV2/cards/SummaryEntityCard';

const OutputPortsWrapper = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    min-width: 100px;
    @media (max-width: ${SCREEN_WIDTH_BREAK_POINT}px) {
        margin: 16px 0 0 0;
    }
`;

const StyledHorizontalList = styled(HorizontalList)`
    flex: 1;
`;

const LoadMoreButton = styled(Card)`
    font-size: 16px;
    font-weight: 400;
    font-family: Mulish;
    padding: 10px 14px;
    color: ${ANTD_GRAY[8]};
`;

const COUNT = 10;

export const OutputPortsSection = () => {
    const [additionalResults, setAdditionalResults] = useState<SearchResult[]>([]);
    const [hasFetchedNewData, setHasFetchedNewData] = useState(false);
    const [start, setStart] = useState(0);
    const { urn } = useEntityData();
    const [listDataProductAssets, { data: additionalData }] = useListDataProductAssetsLazyQuery();
    const variables = {
        urn,
        input: {
            query: '*',
            start: 0,
            count: COUNT,
            filters: [{ field: OUTPUT_PORTS_FIELD, value: 'true' }], // we use this filter hardcoded in list data product assets resolver
        },
    };

    const { data, loading } = useListDataProductAssetsQuery({ variables });
    const numResults = data?.listDataProductAssets?.total;
    const showLoadMoreButton = (numResults || 0) > start + COUNT;
    const finalResults = [...(data?.listDataProductAssets?.searchResults || []), ...additionalResults];

    function loadMore() {
        const newStart = start + COUNT;
        listDataProductAssets({
            variables: {
                ...variables,
                input: {
                    ...variables.input,
                    start: newStart,
                },
            },
        });
        setStart(newStart);
        setHasFetchedNewData(true);
    }

    useEffect(() => {
        if (additionalData && additionalData.listDataProductAssets?.searchResults && hasFetchedNewData) {
            setAdditionalResults([...additionalResults, ...additionalData.listDataProductAssets.searchResults]);
            setHasFetchedNewData(false);
        }
    }, [additionalData, additionalResults, hasFetchedNewData]);

    if (!data || !finalResults?.length) return null;

    return (
        <OutputPortsWrapper>
            <StyledHeaderWrapper>
                <SummaryTabHeaderTitle
                    icon={<OutputIcon style={{ fontSize: 16, color: ANTD_GRAY[8] }} />}
                    title={`Output Ports (${numResults})`}
                />
            </StyledHeaderWrapper>
            <StyledHorizontalList>
                {!loading &&
                    finalResults.map((searchResult) => {
                        const { entity } = searchResult;
                        return <SummaryEntityCard key={entity.urn} entity={entity} />;
                    })}
                {showLoadMoreButton && (
                    <LoadMoreButton onClick={loadMore}>
                        <RefreshIcon style={{ marginRight: 4 }} />
                        Load more
                    </LoadMoreButton>
                )}
                {/* KEEPING THIS COMMENTED UNTIL DESIGN IS READY FOR OUTPUT PORT */}
                {/* <AddOutputPortCard /> */}
            </StyledHorizontalList>
        </OutputPortsWrapper>
    );
};
