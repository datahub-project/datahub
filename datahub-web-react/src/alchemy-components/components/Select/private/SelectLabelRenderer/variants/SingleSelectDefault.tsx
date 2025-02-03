import React from 'react';
import {
    ActionButtonsContainer,
    DescriptionContainer,
    LabelsWrapper,
    Placeholder,
    SelectValue,
} from '../../../components';
import { SelectLabelVariantProps } from '../../../types';

export default function SingleSelectDefault({
    selectedOptions,
    selectedValues,
    placeholder,
    isMultiSelect,
    showDescriptions,
}: SelectLabelVariantProps) {
    return (
        <LabelsWrapper>
            {!selectedValues.length && <Placeholder>{placeholder}</Placeholder>}
            {!isMultiSelect && (
                <>
                    <ActionButtonsContainer>
                        {selectedOptions[0]?.icon}
                        <SelectValue>{selectedOptions[0]?.label}</SelectValue>
                    </ActionButtonsContainer>
                    {showDescriptions && !!selectedValues.length && (
                        <DescriptionContainer>{selectedOptions[0]?.description}</DescriptionContainer>
                    )}
                </>
            )}
        </LabelsWrapper>
    );
}
