import React from 'react';
import { ActionButtonsContainer, HighlightedLabel, LabelsWrapper, SelectValue } from '../../../components';
import { SelectLabelVariantProps } from '../../../types';

export default function MultiSelectLabeled({ selectedOptions, label }: SelectLabelVariantProps) {
    return (
        <LabelsWrapper>
            <ActionButtonsContainer>
                <SelectValue>{label}</SelectValue>
                <HighlightedLabel>{selectedOptions.length}</HighlightedLabel>
            </ActionButtonsContainer>
        </LabelsWrapper>
    );
}
