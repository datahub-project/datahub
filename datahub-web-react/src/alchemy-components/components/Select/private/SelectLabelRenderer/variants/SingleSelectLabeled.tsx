import React from 'react';
import { Pill } from '@components';
import {
    ActionButtonsContainer,
    DescriptionContainer,
    LabelsWrapper,
    Placeholder,
    SelectValue,
} from '../../../components';
import { SelectLabelVariantProps, SelectOption } from '../../../types';

export default function SingleSelectLabeled<OptionType extends SelectOption>({
    selectedOptions,
    selectedValues,
    placeholder,
    showDescriptions,
    label,
}: SelectLabelVariantProps<OptionType>) {
    return (
        <LabelsWrapper shouldShowGap={false}>
            {!selectedValues.length && <Placeholder>{placeholder}</Placeholder>}

            {!!selectedValues.length && (
                <ActionButtonsContainer>
                    <SelectValue>{label}</SelectValue>
                    <Pill label={selectedOptions[0]?.label} size="sm" variant="filled" />
                </ActionButtonsContainer>
            )}

            {showDescriptions && <DescriptionContainer>{selectedOptions[0]?.description}</DescriptionContainer>}
        </LabelsWrapper>
    );
}
