import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../constants';
import { StyledSyntaxHighlighter } from '../../../StyledSyntaxHighlighter';

const Statement = styled.div<{ fullHeight?: boolean; isCompact?: boolean }>`
    background-color: ${ANTD_GRAY[2]};
    height: ${(props) => (props.fullHeight && '378px') || '240px'};
    margin: 0px 0px 4px 0px;
    border-radius: 8px;
    :hover {
        cursor: pointer;
    }
    overflow: auto !important;

    ${(props) =>
        props.isCompact &&
        `
        height: 55px;
        overflow: hidden !important;
        margin: 0;
    `}
`;

const NestedSyntax = styled(StyledSyntaxHighlighter)<{ isCompact?: boolean }>`
    background-color: transparent !important;
    border: none !important;
    margin: 0px !important;
    height: 100% !important;
    overflow: auto !important;
    ::-webkit-scrollbar {
        display: none;
    } !important;

    ${(props) =>
        props.isCompact &&
        `
        overflow: hidden !important;
    `}
`;

export type Props = {
    query: string;
    showDetails: boolean;
    onClickExpand?: (newQuery) => void;
    index?: number;
    isCompact?: boolean;
};

export default function QueryCardQuery({ query, showDetails, onClickExpand, index, isCompact }: Props) {
    return (
        <Statement
            fullHeight={!showDetails}
            onClick={onClickExpand}
            data-testid={`query-content-${index}`}
            isCompact={isCompact}
        >
            <NestedSyntax showLineNumbers language="sql" isCompact={isCompact}>
                {query}
                {query}
            </NestedSyntax>
        </Statement>
    );
}
