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
    removeOption,
    renderCustomSelectedValue,
}: SelectLabelVariantProps<OptionType>) {
    const option = selectedOptions[0];

    return (
        <LabelsWrapper shouldShowGap={false}>
            {!selectedValues?.length && <Placeholder>{placeholder}</Placeholder>}
            {!isMultiSelect && !!selectedValues?.length && (
                <>
                    <ActionButtonsContainer>
                        <SelectValue>
                            {renderCustomSelectedValue
                                ? renderCustomSelectedValue(option, () => removeOption?.(option))
                                : option?.label}
                        </SelectValue>
                    </ActionButtonsContainer>
                    {showDescriptions && <DescriptionContainer>{option?.description}</DescriptionContainer>}
                </>
            )}
        </LabelsWrapper>
    );
}
