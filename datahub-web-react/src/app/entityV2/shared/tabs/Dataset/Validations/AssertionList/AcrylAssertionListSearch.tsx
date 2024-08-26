import { SearchOutlined } from '@ant-design/icons';
import { Input } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { pluralize } from '@src/app/shared/textUtil';

const StyledInput = styled(Input)`
    width: auto;
    background: ${REDESIGN_COLORS.WHITE};
    font-size: 14px;
    font-weight: 500;
    line-height: 24px;
    margin-bottom: 8px;
    color: ${REDESIGN_COLORS.BODY_TEXT};
`;

const MatchLabelText = styled.span`
    font-size: 12px;
    font-weight: 400;
    color: ${REDESIGN_COLORS.DARK_GREY};
    bottom: -12px;
    position: absolute;
    margin: 0;
`;

const SearchContainer = styled.div`
    position: relative;
    --antd-wave-shadow-color: transparent;
    flex: auto;
    display: flex;
    flex-grow: 0;
    align-items: start;
    white-space: nowrap;
    flex-direction: column;

    .ant-input-group-wrapper {
        border-radius: 20px;
        border: 1px solid ${REDESIGN_COLORS.GREY};
        background: #f3f5fa;
    }

    .ant-input-group-wrapper {
        background-color: #ffffff !important;
    }

    .ant-input-wrapper {
        background-color: #ffffff00 !important;
    }

    .ant-input {
        border-radius: 0;
        color: ${REDESIGN_COLORS.BODY_TEXT};
    }
    .ant-input::placeholder {
        color: ${REDESIGN_COLORS.BODY_TEXT} !important;
        opacity: 1;
    }

    .ant-input-affix-wrapper {
        border-radius: 8px;
        border: 1px solid ${REDESIGN_COLORS.SILVER_GREY};
        transition: border-color 0.3s ease-in-out;
        cursor: text !important;
    }

    .ant-input-group-addon {
        border: none;
        background-color: #ffffff00 !important;
        left: 2px;
    }

    .ant-input-affix-wrapper:focus-within,
    .ant-input-affix-wrapper:not(.ant-input-affix-wrapper-disabled):hover {
        border: 1px solid ${REDESIGN_COLORS.TITLE_PURPLE};
    }

    .ant-input-affix-wrapper::selection {
        background: transparent;
    }
`;

interface AcrylAssertionListSearchProps {
    searchText: string;
    debouncedSetFilterText: (event: React.ChangeEvent<HTMLInputElement>) => void;
    matchResultCount: number;
    numRows: number;
}

export const AcrylAssertionListSearch: React.FC<AcrylAssertionListSearchProps> = ({
    searchText,
    debouncedSetFilterText,
    matchResultCount,
    numRows,
}) => {
    return (
        <SearchContainer>
            <StyledInput
                bordered={false}
                defaultValue={searchText}
                placeholder="Search..."
                onChange={debouncedSetFilterText}
                allowClear
                prefix={<SearchOutlined />}
            />
            {searchText && (
                <MatchLabelText>
                    Matched {matchResultCount} {pluralize(matchResultCount, 'assertion')} of {numRows}
                </MatchLabelText>
            )}
        </SearchContainer>
    );
};
