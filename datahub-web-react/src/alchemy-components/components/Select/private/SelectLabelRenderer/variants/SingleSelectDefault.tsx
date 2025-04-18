import React from 'react';

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
