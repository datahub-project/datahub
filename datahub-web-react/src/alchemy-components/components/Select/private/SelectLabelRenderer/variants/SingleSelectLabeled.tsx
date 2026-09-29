import { Pill } from '@components';
import React from 'react';
import styled from 'styled-components';

import {
    ActionButtonsContainer,
    DescriptionContainer,
    LabelsWrapper,
    Placeholder,
    SelectValue,
} from '@components/components/Select/components';
import { SelectLabelVariantProps, SelectOption } from '@components/components/Select/types';

// Keeps the option icon at its natural size instead of getting squeezed
const IconWrapper = styled.span`
    display: inline-flex;
    align-items: center;
    flex-shrink: 0;
`;

export default function SingleSelectLabeled<OptionType extends SelectOption>({
    selectedOptions,
    selectedValues,
    placeholder,
    showDescriptions,
    label,
}: SelectLabelVariantProps<OptionType>) {
    const value = selectedOptions[0]?.value;
    const selectedIcon = selectedOptions[0]?.icon;

    return (
        <LabelsWrapper shouldShowGap={false}>
            {!selectedValues.length && <Placeholder>{placeholder}</Placeholder>}

            {!!selectedValues.length && (
                <ActionButtonsContainer>
                    <SelectValue>{label}</SelectValue>
                    {selectedIcon && <IconWrapper>{selectedIcon}</IconWrapper>}
                    <Pill
                        label={selectedOptions[0]?.label}
                        size="sm"
                        variant="filled"
                        dataTestId={value ? `value-${value}` : undefined}
                    />
                </ActionButtonsContainer>
            )}

            {showDescriptions && <DescriptionContainer>{selectedOptions[0]?.description}</DescriptionContainer>}
        </LabelsWrapper>
    );
}
