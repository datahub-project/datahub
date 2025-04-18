import { Pill } from '@components';
import React from 'react';
<<<<<<< HEAD

=======
import { Pill } from '@components';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105
import {
    ActionButtonsContainer,
    DescriptionContainer,
    LabelsWrapper,
    Placeholder,
    SelectValue,
<<<<<<< HEAD
} from '@components/components/Select/components';
import { SelectLabelVariantProps, SelectOption } from '@components/components/Select/types';
=======
} from '../../../components';
import { SelectLabelVariantProps, SelectOption } from '../../../types';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

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
