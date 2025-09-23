import React from 'react';

import {
    ActionButtonsContainer,
    DescriptionContainer,
    LabelsWrapper,
    Placeholder,
    SelectValue,
} from '@components/components/Select/components';
import { SelectLabelVariantProps, SelectOption } from '@components/components/Select/types';

export default function SingleSelectCustom<OptionType extends SelectOption>({
    selectedOptions,
    selectedValues,
    placeholder,
    isMultiSelect,
    showDescriptions,
    renderCustomSelectedValue,
}: SelectLabelVariantProps<OptionType>) {
    return (
        <LabelsWrapper shouldShowGap={false}>
            {!selectedValues?.length && <Placeholder>{placeholder}</Placeholder>}
            {!isMultiSelect && !!selectedValues?.length && (
                <>
                    <ActionButtonsContainer>
                        <SelectValue>
                            {renderCustomSelectedValue
                                ? renderCustomSelectedValue(selectedOptions[0])
                                : selectedOptions[0]?.label}
                        </SelectValue>
                    </ActionButtonsContainer>
                    {showDescriptions && <DescriptionContainer>{selectedOptions[0]?.description}</DescriptionContainer>}
                </>
            )}
        </LabelsWrapper>
    );
}
