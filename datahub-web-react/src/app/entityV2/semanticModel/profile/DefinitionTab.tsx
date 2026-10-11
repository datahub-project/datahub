import { CodeBlock } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { EmptyTab } from '@app/entityV2/shared/components/styled/EmptyTab';

import { GetSemanticModelQuery } from '@graphql/semanticModel.generated';

const TabContainer = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
    padding: 20px;
    gap: 12px;
    overflow: auto;
`;

const DefinitionCode = styled(CodeBlock)`
    flex: 1;
`;

export function DefinitionTab() {
    const baseEntity = useBaseEntity<GetSemanticModelQuery>();
    const nativeDefinition = baseEntity?.semanticModel?.info?.nativeDefinition;

    if (!nativeDefinition) {
        return (
            <TabContainer>
                <EmptyTab tab="definition" />
            </TabContainer>
        );
    }

    return (
        <TabContainer>
            <DefinitionCode
                data-testid="definition-code-block"
                copyDataTestId="definition-copy-button"
                code={nativeDefinition}
                language="yaml"
                showLineNumbers
                wrap
            />
        </TabContainer>
    );
}
