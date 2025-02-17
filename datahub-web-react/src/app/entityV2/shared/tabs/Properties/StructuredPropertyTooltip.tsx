import React from 'react';
import styled from 'styled-components';
import CardinalityLabel from './CardinalityLabel';
import { PropertyRow } from './types';
import PropertyTypeLabel from '../../../../entity/shared/tabs/Dataset/Schema/components/PropertyTypeLabel';

const ContentWrapper = styled.div`
    font-size: 12px;
`;

const Header = styled.div`
    font-size: 10px;
`;

const Description = styled.div`
    padding-left: 16px;
`;

const NameWrapper = styled.span`
    font-size: 14px;
    font-weight: 600;
`;

const NameLabelWrapper = styled.div`
    display: flex;
    align-items: center;
    white-space: nowrap;
`;

interface Props {
    propertyRow: PropertyRow;
}

export default function StructuredPropertyTooltip({ propertyRow }: Props) {
    const { structuredProperty } = propertyRow;

    if (!structuredProperty) return null;

    return (
        <ContentWrapper>
            <Header>Structured Property</Header>
            <NameLabelWrapper>
                <NameWrapper>
                    {structuredProperty.definition.displayName || structuredProperty.definition.qualifiedName}{' '}
                </NameWrapper>
                {propertyRow.type ? (
                    <PropertyTypeLabel type={propertyRow.type} dataType={propertyRow.dataType} displayTransparent />
                ) : (
                    <span />
                )}
                {structuredProperty?.definition?.allowedValues && (
                    <CardinalityLabel structuredProperty={structuredProperty} />
                )}
            </NameLabelWrapper>
            {structuredProperty.definition.description && (
                <Description>{structuredProperty.definition.description}</Description>
            )}
        </ContentWrapper>
    );
}
