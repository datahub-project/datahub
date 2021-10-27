import React, { useMemo, useState } from 'react';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';

import { Typography } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../constants';
import { SeeMore } from '../../../components/styled/SeeMore';

export type Props = {
    query?: string | null;
};

const QueryText = styled(Typography.Paragraph)`
    margin: 20px;
    &&& pre {
        background-color: ${ANTD_GRAY[2]};
        border: none;
    }
`;

// NOTE: Yes, using `!important` is a shame. However, the SyntaxHighlighter is applying styles directly
// to the component, so there's no way around this
const NestedSyntax = styled(SyntaxHighlighter)`
    background-color: transparent !important;
    border: none !important;
`;

const UNEXPANDED_SIZE = 5;

export default function Query({ query }: Props) {
    const rows = useMemo(() => query?.split('\n'), [query]);

    const unexpandedRows = useMemo(() => rows?.slice(0, UNEXPANDED_SIZE), [rows]);

    const [expanded, setExpanded] = useState((rows?.length || 0) <= UNEXPANDED_SIZE);

    if (!query) {
        return null;
    }

    return (
        <QueryText>
            <pre>
                {!expanded ? (
                    <>
                        <NestedSyntax language="sql">{unexpandedRows?.join('\n')}</NestedSyntax>
                        <SeeMore onClick={() => setExpanded(true)}>···</SeeMore>
                    </>
                ) : (
                    <NestedSyntax language="sql">{query}</NestedSyntax>
                )}
            </pre>
        </QueryText>
    );
}
