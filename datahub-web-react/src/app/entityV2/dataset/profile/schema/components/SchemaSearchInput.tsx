import { Button, Popover, SearchBar } from '@components';
import React from 'react';
import styled from 'styled-components';

import SchemaFilterSelectContent from '@app/entityV2/dataset/profile/schema/components/SchemaFilterSelectContent';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { SchemaFilterType } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/filterSchemaRows';
import { pluralize } from '@app/shared/textUtil';

const MatchLabelText = styled.span`
    font-size: 12px;
    font-style: normal;
    font-weight: 700;
    color: ${REDESIGN_COLORS.DARK_GREY};
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
    return (
        <SearchContainer>
            <SearchBar
                value={searchInput}
                disabled={schemaFilterTypes.length === 0}
                placeholder="Search"
                onChange={setSearchInput}
                allowClear
                onKeyDown={(e) => {
                    if (e.code === 'Enter' && highlightedMatchIndex !== null && matches.length > 0) {
                        setHighlightedMatchIndex((highlightedMatchIndex + 1) % matches.length);
                    }
                }}
                width="300px"
            />
            <Popover
                placement="bottom"
                open={schemaFilterSelectOpen}
                onOpenChange={(val) => setSchemaFilterSelectOpen(val)}
                content={
                    <SchemaFilterSelectContent
                        close={() => setSchemaFilterSelectOpen(false)}
                        schemaFilterTypes={schemaFilterTypes}
                        setSchemaFilterTypes={setSchemaFilterTypes}
                    />
                }
                trigger="click"
                overlayInnerStyle={{ padding: 0 }}
            >
                <Button
                    variant="text"
                    color={schemaFilterTypes.length < 4 ? 'violet' : 'gray'}
                    icon={{ icon: 'FadersHorizontal', source: 'phosphor', size: '2xl' }}
                />
            </Popover>
            {searchInput.length > 0 && (
                <MatchLabelText>
                    Matched {matches.length} {pluralize(matches.length, 'column')} of {numRows}
                </MatchLabelText>
            )}
        </SearchContainer>
    );
};

export default SchemaSearchInput;
