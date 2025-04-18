import { Pill } from '@components';
import React from 'react';
<<<<<<< HEAD

import { ActionButtonsContainer, LabelsWrapper, SelectValue } from '@components/components/Select/components';
import { SelectLabelVariantProps, SelectOption } from '@components/components/Select/types';

=======
import { Pill } from '@components';
import { ActionButtonsContainer, LabelsWrapper, SelectValue } from '../../../components';
import { SelectLabelVariantProps, SelectOption } from '../../../types';

>>>>>>> dbad52283b070c7cc136306c1553770db2f72105
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
