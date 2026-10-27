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
// alongside the truncating SelectValue text.
const IconWrapper = styled.span`
    display: inline-flex;
    flex-shrink: 0;
`;

export default function SingleSelectDefault<OptionType extends SelectOption>({
    selectedOptions,
    selectedValues,
    placeholder,
    isMultiSelect,
    showDescriptions,
}: SelectLabelVariantProps<OptionType>) {
    const value = selectedOptions[0]?.value;

    return (
        <LabelsWrapper shouldShowGap={false}>
            {!selectedValues.length && <Placeholder>{placeholder}</Placeholder>}
            {!isMultiSelect && (
                <>
                    <ActionButtonsContainer>
                        {selectedOptions[0]?.icon && <IconWrapper>{selectedOptions[0]?.icon}</IconWrapper>}
                        <SelectValue data-testid={value ? `value-${value}` : undefined}>
                            {selectedOptions[0]?.label}
                        </SelectValue>
                    </ActionButtonsContainer>
                    {showDescriptions && !!selectedValues.length && (
                        <DescriptionContainer>{selectedOptions[0]?.description}</DescriptionContainer>
                    )}
                </>
            )}
        </LabelsWrapper>
    );
}
