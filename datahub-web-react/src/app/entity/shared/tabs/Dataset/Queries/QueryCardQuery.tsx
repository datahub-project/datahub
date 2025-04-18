import React from 'react';
import styled from 'styled-components';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { ANTD_GRAY } from '../../../constants';

const Statement = styled.div<{ fullHeight?: boolean }>`
    background-color: ${ANTD_GRAY[2]};
    height: ${(props) => (props.fullHeight && '378px') || '240px'};
    margin: 0px 0px 4px 0px;
    border-radius: 8px;
    :hover {
        cursor: pointer;
    }
`;

const NestedSyntax = styled(SyntaxHighlighter)`
    background-color: transparent !important;
    border: none !important;
    margin: 0px !important;
    height: 100% !important;
    overflow: auto !important;
    ::-webkit-scrollbar {
        display: none;
    } !important;
`;

export type Props = {
    query: string;
    showDetails: boolean;
    onClickExpand?: (newQuery) => void;
    index?: number;
};

export default function QueryCardQuery({ query, showDetails, onClickExpand, index }: Props) {
    return (
        <Statement fullHeight={!showDetails} onClick={onClickExpand} data-testid={`query-content-${index}`}>
            <NestedSyntax showLineNumbers language="sql">
                {query}
            </NestedSyntax>
        </Statement>
    );
}
