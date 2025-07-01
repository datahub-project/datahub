import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { SEARCH_RESULTS_FILTERS_V2_INTRO } from '@app/onboarding/config/SearchOnboardingConfig';
import SearchFilters from '@app/searchV2/filters/SearchFilters';
import { UnionType } from '@app/searchV2/utils/constants';
import { DownloadSearchResults, DownloadSearchResultsInput } from '@app/searchV2/utils/types';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

import { FacetFilterInput, FacetMetadata } from '@types';

const Section = styled.div<{ removePadding?: boolean; $isShowNavBarRedesign?: boolean }>`
    padding: ${(props) => {
        if (props.$isShowNavBarRedesign) {
            return props.removePadding ? '8px 20px 4px 20px' : '5px 5px 8px 5px';
        }
        return props.removePadding ? '8px 20px 4px 20px' : '8px 12px 0px 12px';
    }};
    position: relative;
`;

interface Props {
    loading: boolean;
    availableFilters: FacetMetadata[];
    activeFilters: FacetFilterInput[];
    unionType: UnionType;
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    onClearFilters: () => void;
    onChangeUnionType: (unionType: UnionType) => void;
    query: string;
    viewUrn?: string;
    totalResults: number;
    setShowSelectMode?: (showSelectMode: boolean) => any;
    downloadSearchResults: (input: DownloadSearchResultsInput) => Promise<DownloadSearchResults | null | undefined>;
}

export default function SearchFiltersSection({
    loading,
    availableFilters,
    activeFilters,
    unionType,
    onChangeFilters,
    onClearFilters,
    onChangeUnionType,
    query,
    viewUrn,
    totalResults,
    setShowSelectMode,
    downloadSearchResults,
}: Props) {
    const [finalAvailableFilters, setFinalAvailableFilters] = useState(availableFilters);
    const isShowNavBarRedesign = useShowNavBarRedesign();

    /**
     * Only update the active filters if we are done loading. Prevents jitter!
     */
    useEffect(() => {
        if (!loading && finalAvailableFilters !== availableFilters) {
            setFinalAvailableFilters(availableFilters);
        }
    }, [availableFilters, loading, finalAvailableFilters]);

    return (
        <Section
            id={SEARCH_RESULTS_FILTERS_V2_INTRO}
            data-testid="search-filters-v2"
            $isShowNavBarRedesign={isShowNavBarRedesign}
        >
            <SearchFilters
                loading={loading}
                availableFilters={finalAvailableFilters}
                activeFilters={activeFilters}
                unionType={unionType}
                onChangeFilters={onChangeFilters}
                onClearFilters={onClearFilters}
                onChangeUnionType={onChangeUnionType}
                query={query}
                viewUrn={viewUrn}
                totalResults={totalResults}
                setShowSelectMode={setShowSelectMode}
                downloadSearchResults={downloadSearchResults}
            />
        </Section>
    );
}
