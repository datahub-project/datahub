import { CodeBlock } from '@components';
import React from 'react';
import styled from 'styled-components';

import DynamicPropertiesTab from '@app/entityV2/shared/tabs/Entity/weaklyTypedAspects/DynamicPropertiesTab';
import DynamicTabularTab from '@app/entityV2/shared/tabs/Entity/weaklyTypedAspects/DynamicTabularTab';

import { AspectRenderSpec } from '@types';

type Props = {
    payload: string | undefined | null;
    type: string | undefined | null;
    renderSpec: AspectRenderSpec | undefined | null;
};

const QueryText = styled.div`
    margin: 20px;
`;

const SYNTAX_LANGUAGE_JSON = 'json';
const EMPTY_JSON_OBJECT = '{}';

export default function DynamicTab({ renderSpec, payload, type }: Props) {
    if (type === 'tabular') {
        return <DynamicTabularTab payload={payload} tableKey={renderSpec?.key} />;
    }
    if (type === 'properties') {
        return <DynamicPropertiesTab payload={payload} />;
    }

    // Default fallback behavior
    return (
        <QueryText>
            <CodeBlock
                code={JSON.stringify(JSON.parse(payload || EMPTY_JSON_OBJECT), null, 2)}
                language={SYNTAX_LANGUAGE_JSON}
                variant="embedded"
                showHeader={false}
                showCopy={false}
                showFormat={false}
            />
        </QueryText>
    );
}
