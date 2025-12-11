/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
    const [localSearchText, setLocalSearchText] = useState(searchText);

    useDebounce(
        () => {
            debouncedSetFilterText(localSearchText);
        },
        500,
        [localSearchText],
    );

    return (
        <SearchContainer>
            <StyledInput
                value={localSearchText}
                placeholder={options?.placeholder || 'Search...'}
                onChange={(e) => setLocalSearchText(e.target.value)}
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
