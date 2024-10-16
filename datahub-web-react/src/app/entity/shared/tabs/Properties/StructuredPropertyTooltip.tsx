import React from 'react';
import styled from 'styled-components';
import { StructuredPropertyEntity } from '../../../../../types.generated';

const ContentWrapper = styled.div`
    font-size: 12px;
`;

const Header = styled.div`
    font-size: 10px;
`;

const Description = styled.div`
    padding-left: 16px;
`;

interface Props {
    structuredProperty: StructuredPropertyEntity;
}

export default function StructuredPropertyTooltip({ structuredProperty }: Props) {
    return (
        <ContentWrapper>
            <Header>Structured Property</Header>
            <div>{structuredProperty.definition.displayName || structuredProperty.definition.qualifiedName}</div>
            {structuredProperty.definition.description && (
                <Description>{structuredProperty.definition.description}</Description>
            )}
        </ContentWrapper>
    );
}
