import React from 'react';
import { ActionButtonsContainer, HighlightedLabel, LabelsWrapper, SelectValue } from '../../../components';
import { SelectLabelVariantProps } from '../../../types';

export default function MultiSelectLabeled({ selectedOptions, label }: SelectLabelVariantProps) {
    return (
        <LabelsWrapper>
            <ActionButtonsContainer>
                <SelectValue>{label}</SelectValue>
                {selectedOptions.length > 0 && <HighlightedLabel>{selectedOptions.length}</HighlightedLabel>}
            </ActionButtonsContainer>
        </LabelsWrapper>
    );
}
