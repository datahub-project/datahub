import { Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { FilterField, FilterValue } from '@app/searchV2/filters/types';
import { useFilterDisplayName } from '@app/searchV2/filters/utils';
import TextValueInput from '@app/searchV2/filters/value/TextValueInput';

const Container = styled.div<{ $shouldAddWrapperStyles: boolean }>`
    ${({ $shouldAddWrapperStyles, theme }) =>
        $shouldAddWrapperStyles &&
        `
        padding: 16px;
        background-color: ${theme.colors.bg};
        box-shadow: ${theme.colors.shadowMd};
        border-radius: 12px;
    `}
`;

const Title = styled(Text).attrs({ weight: 'semiBold', color: 'text' })``;

interface Props {
    field: FilterField;
    values: FilterValue[];
    onChangeValues: (newValues: FilterValue[]) => void;
    isRenderedInSubMenu?: boolean;
}

export default function TextValueMenu({ field, values, onChangeValues, isRenderedInSubMenu }: Props) {
    const { t } = useTranslation('search');
    // Resolve the human-readable label (e.g. "UUID Filter Test") rather than the raw field name
    // (structuredProperties.uuidFilterTest), consistent with the enum/entity menus.
    const displayName = useFilterDisplayName(field);
    const value = values.length > 0 ? values[0].displayName || values[0].value : '';

    return (
        <Container $shouldAddWrapperStyles={!isRenderedInSubMenu}>
            <Title>{t('filters.filterBy', { name: displayName })}</Title>
            <TextValueInput
                name={displayName}
                value={value}
                onChangeValue={(v) => onChangeValues([{ value: v, entity: null }])}
            />
        </Container>
    );
}
