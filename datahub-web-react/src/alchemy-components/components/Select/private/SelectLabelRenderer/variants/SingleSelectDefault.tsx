import React from 'react';
import {
    ActionButtonsContainer,
    DescriptionContainer,
    LabelsWrapper,
    Placeholder,
    SelectValue,
} from '../../../components';
import { SelectLabelVariantProps, SelectOption } from '../../../types';

export default function SingleSelectDefault<OptionType extends SelectOption>({
    selectedOptions,
    selectedValues,
    placeholder,
    isMultiSelect,
    showDescriptions,
}: SelectLabelVariantProps<OptionType>) {
    return (
        <LabelsWrapper shouldShowGap={false}>
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
