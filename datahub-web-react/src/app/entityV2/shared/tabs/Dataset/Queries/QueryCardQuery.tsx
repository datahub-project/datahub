import { CodeBlock } from '@components';
import React from 'react';
import styled from 'styled-components';

import { CodeBlockContent } from '@components/components/CodeBlock/components';

import { SQL_LANGUAGE } from '@app/entityV2/shared/tabs/Dataset/Queries/utils/constants';

const Statement = styled.div<{ fullHeight?: boolean; isCompact?: boolean }>`
    background-color: ${(props) => props.theme.colors.bgSurface};
    height: ${(props) => (props.fullHeight && '378px') || '240px'};
    margin: 0px 0px 4px 0px;
    border-radius: 8px;
    :hover {
        cursor: pointer;
    }
    overflow: auto;

    ${(props) =>
        props.isCompact &&
        `
        height: 55px;
        overflow: hidden;
        margin: 0;
    `}
`;

// The card is a fixed-height preview, so the scrollbar would sit on top of the
// clipped SQL rather than alongside it.
const NestedCode = styled(CodeBlock)`
    height: 100%;

    ${CodeBlockContent}::-webkit-scrollbar {
        display: none;
    }
`;

type Props = {
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
            <NestedCode
                code={query}
                language={SQL_LANGUAGE}
                variant="embedded"
                showHeader={false}
                showCopy={false}
                showFormat={false}
                showLineNumbers
                maxHeight="100%"
                overflow={isCompact ? 'hidden' : 'auto'}
            />
        </Statement>
    );
}
