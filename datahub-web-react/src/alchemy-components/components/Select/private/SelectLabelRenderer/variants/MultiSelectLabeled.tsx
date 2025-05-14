import { Pill } from '@components';
import React from 'react';

import { ActionButtonsContainer, LabelsWrapper, SelectValue } from '@components/components/Select/components';
import { SelectLabelVariantProps, SelectOption } from '@components/components/Select/types';

export default function MultiSelectLabeled<OptionType extends SelectOption>({
    selectedOptions,
    label,
}: SelectLabelVariantProps<OptionType>) {
    return (
        <LabelsWrapper shouldShowGap={false}>
            <ActionButtonsContainer>
                <SelectValue>{label}</SelectValue>
                {selectedOptions.length > 0 && <Pill label={`${selectedOptions.length}`} size="sm" variant="filled" />}
            </ActionButtonsContainer>
        </LabelsWrapper>
    );
}
