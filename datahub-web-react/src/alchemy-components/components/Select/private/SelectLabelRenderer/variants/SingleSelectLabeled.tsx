import { Pill } from '@components';
import React from 'react';

import {
    ActionButtonsContainer,
    DescriptionContainer,
    LabelsWrapper,
    Placeholder,
    SelectValue,
} from '@components/components/Select/components';
import { SelectLabelVariantProps } from '@components/components/Select/types';

export default function SingleSelectLabeled({
    selectedOptions,
    selectedValues,
    placeholder,
    showDescriptions,
    label,
}: SelectLabelVariantProps) {
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
