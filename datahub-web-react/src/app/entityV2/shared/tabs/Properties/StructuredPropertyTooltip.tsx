import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import PropertyTypeLabel from '@app/entity/shared/tabs/Dataset/Schema/components/PropertyTypeLabel';
import CardinalityLabel from '@app/entityV2/shared/tabs/Properties/CardinalityLabel';
import { PropertyRow } from '@app/entityV2/shared/tabs/Properties/types';

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
    const { t } = useTranslation('entity.profile.tabs');
    const { structuredProperty } = propertyRow;

    if (!structuredProperty) return null;

    return (
        <ContentWrapper>
            <Header>{t('properties.structuredProperty.title')}</Header>
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
