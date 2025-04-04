import React from 'react';
import { Pill } from '@components';
import { ActionButtonsContainer, LabelsWrapper, SelectValue } from '../../../components';
import { SelectLabelVariantProps } from '../../../types';

export default function MultiSelectLabeled({ selectedOptions, label }: SelectLabelVariantProps) {
    return (
        <LabelsWrapper shouldShowGap={false}>
            <ActionButtonsContainer>
                <SelectValue>{label}</SelectValue>
                {selectedOptions.length > 0 && <Pill label={`${selectedOptions.length}`} size="sm" variant="filled" />}
            </ActionButtonsContainer>
        </LabelsWrapper>
    );
}
