import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { FilterField, FilterValue } from '@app/searchV2/filters/types';
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

const Title = styled(Text).attrs({ weight: 'semiBold' })`
    color: ${({ theme }) => theme.colors.textSecondary};
`;

interface Props {
    field: FilterField;
    values: FilterValue[];
    onChangeValues: (newValues: FilterValue[]) => void;
    isRenderedInSubMenu?: boolean;
}

export default function TextValueMenu({ field, values, onChangeValues, isRenderedInSubMenu }: Props) {
    const { displayName } = field;
    const value = values.length > 0 ? values[0].displayName || values[0].value : '';

    return (
        <Container $shouldAddWrapperStyles={!isRenderedInSubMenu}>
            <Title>{`Filter by ${displayName}`}</Title>
            <TextValueInput
                name={displayName}
                value={value}
                onChangeValue={(v) => onChangeValues([{ value: v, entity: null }])}
            />
        </Container>
    );
}
