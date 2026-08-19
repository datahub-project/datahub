import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useDebounce } from 'react-use';

import { MatchLabelText, SearchContainer, StyledInput } from '@app/entityV2/shared/components/search/styledComponents';

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
    const { t } = useTranslation('entity.shared.components');
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
                placeholder={options?.placeholder || t('search.searchPlaceholder')}
                onChange={(e) => setLocalSearchText(e.target.value)}
                icon={options?.hidePrefix ? undefined : { icon: MagnifyingGlass }}
                label=""
            />
            {searchText && !options?.hideMatchCountText && (
                <MatchLabelText data-testid="inline-search-matched-result-text">
                    {t('search.matched', {
                        count: matchResultCount,
                        matchResultCount,
                        entityType: entityTypeName,
                        numRows,
                    })}
                </MatchLabelText>
            )}
        </SearchContainer>
    );
};
