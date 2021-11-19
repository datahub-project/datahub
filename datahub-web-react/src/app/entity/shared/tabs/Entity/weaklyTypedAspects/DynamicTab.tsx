import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';

import { ANTD_GRAY } from '../../../constants';
import DynamicTabularTab from './DynamicTabularTab';
import DynamicPropertiesTab from './DynamicPropertiesTab';
import { AspectRenderSpec } from '../../../../../../types.generated';

type Props = {
    payload: string | undefined | null;
    type: string | undefined | null;
    renderSpec: AspectRenderSpec | undefined | null;
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

export default function DynamicTab({ renderSpec, payload, type }: Props) {
    if (type === 'tabular') {
        return <DynamicTabularTab payload={payload} tableKey={renderSpec?.key} />;
    }
    if (type === 'properties') {
        return <DynamicPropertiesTab payload={payload} />;
    }

    // Default fallback behavior
    return (
        <>
            <QueryText>
                <pre>
                    <NestedSyntax language="json">{JSON.stringify(JSON.parse(payload || '{}'), null, 2)}</NestedSyntax>
                </pre>
            </QueryText>
        </>
    );
}
