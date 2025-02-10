import { FilterOutlined, SearchOutlined } from '@ant-design/icons';
import { Input } from 'antd';
import { Popover } from '@components';
import React from 'react';
import styled from 'styled-components';
import { pluralize } from '../../../../../shared/textUtil';
import { REDESIGN_COLORS } from '../../../../shared/constants';
import { SchemaFilterType } from '../../../../shared/tabs/Dataset/Schema/utils/filterSchemaRows';
import SchemaFilterSelectContent from './SchemaFilterSelectContent';

const StyledInput = styled(Input)`
    max-width: 300px;
    background: ${REDESIGN_COLORS.LIGHT_GREY};
    margin-top: 5px;
    font-size: 14px;
    font-weight: 500;
    line-height: 24px;
    color: ${REDESIGN_COLORS.DARK_GREY};
`;

const MatchLabelText = styled.span`
    font-size: 12px;
    font-style: normal;
    font-weight: 700;
    color: ${REDESIGN_COLORS.DARK_GREY};
    padding-left: 10px;
    margin-top: 5px;
`;

const SearchContainer = styled.span`
    --antd-wave-shadow-color: transparent;
    flex: auto;
    white-space: nowrap;
    display: flex;
    align-items: center;

    .ant-input-group-wrapper {
        border-radius: 20px;
        border: 1px solid ${REDESIGN_COLORS.GREY};
        background: #f3f5fa;
    }

    .ant-input-group-wrapper {
        background-color: #ffffff00 !important;
    }

    .ant-input-wrapper {
        background-color: #ffffff00 !important;
    }

    .ant-input {
        border-radius: 0;
    }

    .ant-input-affix-wrapper {
        border-radius: 20px;
        border: none;
    }

    .ant-input-group-addon {
        border: none;
        background-color: #ffffff00 !important;
        left: 2px;
    }

    .ant-input-affix-wrapper:focus {
        border: none;
    }

    .ant-input-affix-wrapper:not(.ant-input-affix-wrapper-disabled):hover {
        border: none;
    }

    .ant-input-affix-wrapper::selection {
        background: transparent;
    }
`;

const StyledPopover = styled(Popover)`
    border-radius: 50%;
    background: ${REDESIGN_COLORS.GREY};
`;

const StyledFilterIcon = styled(FilterOutlined)<{ $hasFiltered: boolean }>`
    cursor: pointer;
    margin-left: -14px;
    margin-right: -14px;
    margin-top: -12px;
    margin-bottom: -12px;
    padding-left: 14px;
    padding-right: 14px;
    padding-top: 12px;
    padding-bottom: 12px;
    border-radius: 50%;
    color: ${(props) => (props.$hasFiltered ? props.theme.styles['primary-color'] : 'inherit')};

    :hover {
        color: ${(props) => (!props.$hasFiltered ? props.theme.styles['primary-color'] : 'inherit')};
    }
`;

interface SchemaSearchProps {
    schemaFilterTypes: SchemaFilterType[];
    setSchemaFilterTypes: (filters: SchemaFilterType[]) => void;
    schemaFilter: string;
    debouncedSetFilterText: (event: React.ChangeEvent<HTMLInputElement>) => void;
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
    schemaFilter,
    debouncedSetFilterText,
    matches,
    highlightedMatchIndex,
    setHighlightedMatchIndex,
    schemaFilterSelectOpen,
    setSchemaFilterSelectOpen,
    numRows,
}: SchemaSearchProps) => {
    const schemaFilterTypeSelectPrompt = (
        <StyledPopover
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
            <StyledFilterIcon
                $hasFiltered={schemaFilterTypes.length < 4}
                style={{ cursor: 'pointer', margin: '-10px', padding: '10px' }}
            />
        </StyledPopover>
    );

    return (
        <SearchContainer>
            <StyledInput
                bordered={false}
                disabled={schemaFilterTypes.length === 0}
                addonAfter={schemaFilterTypeSelectPrompt}
                defaultValue={schemaFilter}
                placeholder="Search"
                onChange={debouncedSetFilterText}
                allowClear
                prefix={<SearchOutlined />}
                onKeyDown={(e) => {
                    if (e.code === 'Enter' && highlightedMatchIndex !== null && matches.length > 0) {
                        setHighlightedMatchIndex((highlightedMatchIndex + 1) % matches.length);
                    }
                }}
            />
            {schemaFilter.length > 0 && (
                <MatchLabelText>
                    Matched {matches.length} {pluralize(matches.length, 'column')} of {numRows}
                </MatchLabelText>
            )}
        </SearchContainer>
    );
};

export default SchemaSearchInput;
