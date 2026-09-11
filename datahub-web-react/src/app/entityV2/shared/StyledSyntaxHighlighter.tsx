import React, { CSSProperties, ReactNode } from 'react';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import styled, { useTheme } from 'styled-components';

import { getCodeBlockPrismStyle } from '@components/components/CodeBlock/prismTheme';

interface ThemedSyntaxHighlighterProps {
    children?: ReactNode;
    className?: string;
    language?: string;
    showLineNumbers?: boolean;
    style?: Record<string, CSSProperties>;
    [key: string]: unknown;
}

/**
 * Legacy Prism highlighter. Prefer alchemy `CodeBlock` for new UI; this wrapper
 * only exists so grandfathered call sites pick up the same semantic Prism tokens
 * until they are swapped. Remaining consumers: query cards/modal, weakly-typed
 * aspects, chart summary query, and semantic-model definition. FileNodeView has
 * a separate Prism instance that applies the same theme via attrs.
 *
 * react-syntax-highlighter falls back to a hardcoded light Prism theme when no
 * `style` is supplied, which leaves near-black token text on dark surfaces.
 * Callers can still pass their own `style`.
 */
const ThemedSyntaxHighlighter = ({ style, ...props }: ThemedSyntaxHighlighterProps) => {
    const theme = useTheme();
    return <SyntaxHighlighter style={style ?? getCodeBlockPrismStyle(theme.colors)} {...props} />;
};

export const StyledSyntaxHighlighter = styled(ThemedSyntaxHighlighter)`
    span {
        font-family: 'Roboto Mono', monospace !important;
    }
`;
