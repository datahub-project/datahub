import React from 'react';
import {
    ActionButtonsContainer,
    DescriptionContainer,
    HighlightedLabel,
    LabelsWrapper,
    Placeholder,
    SelectValue,
} from '../../../components';
import { SelectLabelVariantProps } from '../../../types';

export default function SingleSelectLabeled({
    selectedOptions,
    selectedValues,
    placeholder,
    showDescriptions,
    label,
}: SelectLabelVariantProps) {
    return (
        <LabelsWrapper>
            {!selectedValues.length && <Placeholder>{placeholder}</Placeholder>}

            {!!selectedValues.length && (
                <ActionButtonsContainer>
                    <SelectValue>{label}</SelectValue>
                    <HighlightedLabel>{selectedOptions[0]?.label}</HighlightedLabel>
                </ActionButtonsContainer>
            )}

            {showDescriptions && <DescriptionContainer>{selectedOptions[0]?.description}</DescriptionContainer>}
        </LabelsWrapper>
    );
}
