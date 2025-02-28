import { SearchOutlined } from '@ant-design/icons';
import React from 'react';
import { pluralize } from '@src/app/shared/textUtil';
import { MatchLabelText, SearchContainer, StyledInput } from './styledComponents';

interface InlineListSearchProps {
    searchText: string;
    debouncedSetFilterText: (event: React.ChangeEvent<HTMLInputElement>) => void;
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
    return (
        <SearchContainer>
            <StyledInput
                bordered={false}
                value={searchText}
                placeholder={options?.placeholder || 'Search...'}
                onChange={debouncedSetFilterText}
                allowClear
                prefix={!options?.hidePrefix && <SearchOutlined />}
            />
            {searchText && !options?.hideMatchCountText && (
                <MatchLabelText>
                    Matched {matchResultCount} {pluralize(matchResultCount, entityTypeName)} of {numRows}
                </MatchLabelText>
            )}
        </SearchContainer>
    );
};
