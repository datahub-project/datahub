import React, { useState } from 'react';
import { useDebounce } from 'react-use';

import { MatchLabelText, SearchContainer, StyledInput } from '@app/entityV2/shared/components/search/styledComponents';
import { pluralize } from '@src/app/shared/textUtil';

interface InlineListSearchProps {
    searchText: string;
    debouncedSetFilterText: (value: string) => void;
    matchResultCount: number;
    numRows: number;
    options?: {
        hidePrefix?: boolean;
        placeholder?: string;
        allowClear?: boolean;
        hideMatchCountText?: boolean;
    };
    entityTypeName: string;
}

export const InlineListSearch: React.FC<InlineListSearchProps> = ({
    searchText,
    debouncedSetFilterText,
    matchResultCount,
    numRows,
    entityTypeName,
    options,
}) => {
    const [debouncedSearchText, setDebouncedSearchText] = useState(searchText);
    useDebounce(
        () => {
            debouncedSetFilterText(debouncedSearchText);
        },
        500,
        [debouncedSearchText],
    );
    return (
        <SearchContainer>
            <StyledInput
                value={searchText}
                placeholder={options?.placeholder || 'Search...'}
                onChange={(e) => setDebouncedSearchText(e.target.value)}
                icon={options?.hidePrefix ? undefined : { icon: 'MagnifyingGlass', source: 'phosphor' }}
                label=""
            />
            {searchText && !options?.hideMatchCountText && (
                <MatchLabelText data-testid="inline-search-matched-result-text">
                    Matched {matchResultCount} {pluralize(matchResultCount, entityTypeName)} of {numRows}
                </MatchLabelText>
            )}
        </SearchContainer>
    );
};
