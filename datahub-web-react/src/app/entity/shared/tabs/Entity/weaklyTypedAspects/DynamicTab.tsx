/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React from 'react';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import DynamicPropertiesTab from '@app/entity/shared/tabs/Entity/weaklyTypedAspects/DynamicPropertiesTab';
import DynamicTabularTab from '@app/entity/shared/tabs/Entity/weaklyTypedAspects/DynamicTabularTab';

import { AspectRenderSpec } from '@types';

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
