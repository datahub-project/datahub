import { Button, SearchBar } from '@components';
import { FadersHorizontal } from '@phosphor-icons/react/dist/csr/FadersHorizontal';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import SchemaFilterMenu from '@app/entityV2/dataset/profile/schema/components/SchemaFilterMenu';
import { SchemaFilterType } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/filterSchemaRows';

const MatchLabelText = styled.span`
    font-size: 12px;
    font-style: normal;
    font-weight: 700;
    color: ${(props) => props.theme.colors.textSecondary};
    padding-left: 10px;
    margin-top: 5px;
`;

const SearchContainer = styled.span`
    display: flex;
    align-items: center;
    gap: 8px;
    white-space: nowrap;
`;

interface SchemaSearchProps {
    schemaFilterTypes: SchemaFilterType[];
    setSchemaFilterTypes: (filters: SchemaFilterType[]) => void;
    searchInput: string;
    setSearchInput: (value: string) => void;
    matches: string[];
    highlightedMatchIndex: number | null;
    setHighlightedMatchIndex: (val: number | null) => void;
    schemaFilterSelectOpen: boolean;
    setSchemaFilterSelectOpen: (val: boolean) => void;
    numRows: number;
}

const SchemaSearchInput: React.FC<SchemaSearchProps> = ({
    schemaFilterTypes,
    setSchemaFilterTypes,
    searchInput,
    setSearchInput,
    matches,
    highlightedMatchIndex,
    setHighlightedMatchIndex,
    schemaFilterSelectOpen,
    setSchemaFilterSelectOpen,
    numRows,
}: SchemaSearchProps) => {
    const { t } = useTranslation('entity.types');
    const { t: tc } = useTranslation('common.actions');
    return (
        <SearchContainer>
            <SearchBar
                value={searchInput}
                disabled={schemaFilterTypes.length === 0}
                placeholder={tc('search')}
                onChange={setSearchInput}
                allowClear
                onKeyDown={(e) => {
                    if (e.code === 'Enter' && highlightedMatchIndex !== null && matches.length > 0) {
                        setHighlightedMatchIndex((highlightedMatchIndex + 1) % matches.length);
                    }
                }}
                width="300px"
            />
            <SchemaFilterMenu
                schemaFilterTypes={schemaFilterTypes}
                setSchemaFilterTypes={setSchemaFilterTypes}
                open={schemaFilterSelectOpen}
                setOpen={setSchemaFilterSelectOpen}
            >
                <Button
                    variant="text"
                    color={schemaFilterTypes.length < 4 ? 'violet' : 'gray'}
                    icon={{ icon: FadersHorizontal, size: '2xl' }}
                />
            </SchemaFilterMenu>
            {searchInput.length > 0 && (
                <MatchLabelText>{t('dataset.matchedColumnsCount', { count: matches.length, numRows })}</MatchLabelText>
            )}
        </SearchContainer>
    );
};

export default SchemaSearchInput;
