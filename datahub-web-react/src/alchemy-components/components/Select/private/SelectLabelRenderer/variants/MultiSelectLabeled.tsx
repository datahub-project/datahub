import React from 'react';
import { ActionButtonsContainer, HighlightedLabel, LabelsWrapper, Placeholder, SelectValue } from '../../../components';
import { SelectLabelVariantProps } from '../../../types';

export default function MultiSelectLabeled({
    selectedOptions,
    selectedValues,
    placeholder,
    label,
}: SelectLabelVariantProps) {
    return (
        <LabelsWrapper>
            {!selectedValues.length && <Placeholder>{placeholder}</Placeholder>}

            <ActionButtonsContainer>
                <SelectValue>{label}</SelectValue>
                <HighlightedLabel>{selectedOptions.length}</HighlightedLabel>
            </ActionButtonsContainer>
        </LabelsWrapper>
    );
}
