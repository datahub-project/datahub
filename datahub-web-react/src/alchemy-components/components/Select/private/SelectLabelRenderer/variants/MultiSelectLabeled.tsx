import { Pill } from '@components';
import React from 'react';

import { ActionButtonsContainer, LabelsWrapper, SelectValue } from '@components/components/Select/components';
import { SelectLabelVariantProps } from '@components/components/Select/types';

export default function MultiSelectLabeled({ selectedOptions, label }: SelectLabelVariantProps) {
    return (
        <LabelsWrapper shouldShowGap={false}>
            <ActionButtonsContainer>
                <SelectValue>{label}</SelectValue>
                <Pill label={`${selectedOptions.length}`} size="sm" variant="filled" />
            </ActionButtonsContainer>
        </LabelsWrapper>
    );
}
