import { Tooltip } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { PropertyTypeBadge } from '@app/entity/shared/tabs/Dataset/Schema/components/PropertyTypeLabel';
import { getStructuredPropertyValue } from '@app/entity/shared/utils';

import { PropertyCardinality, StructuredPropertyEntity } from '@types';

const Header = styled.div`
    font-size: 10px;
`;

const List = styled.ul`
    padding: 0 24px;
    max-height: 500px;
    overflow: auto;
`;

interface Props {
    structuredProperty: StructuredPropertyEntity;
}

export default function CardinalityLabel({ structuredProperty }: Props) {
    const theme = useTheme();
    const { t } = useTranslation('entity.profile.tabs');
    const labelText =
        structuredProperty.definition.cardinality === PropertyCardinality.Single
            ? t('properties.cardinality.singleSelect')
            : t('properties.cardinality.multiSelect');

    return (
        <Tooltip
            color={theme.colors.bgTooltip}
            title={
                <>
                    <Header>{t('properties.optionsTooltip.title')}</Header>
                    <List>
                        {structuredProperty.definition.allowedValues?.map((value) => (
                            <li>{getStructuredPropertyValue(value.value)}</li>
                        ))}
                    </List>
                </>
            }
        >
            <PropertyTypeBadge count={labelText} displayTransparent />
        </Tooltip>
    );
}
