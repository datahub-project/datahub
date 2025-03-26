import React from 'react';
import {
    ActionButtonsContainer,
    DescriptionContainer,
    LabelsWrapper,
    Placeholder,
    SelectValue,
} from '../../../components';
import { SelectLabelVariantProps } from '../../../types';

export default function SingleSelectCustom({
    selectedOptions,
    selectedValues,
    placeholder,
    isMultiSelect,
    showDescriptions,
    renderCustomSelectedValue,
}: SelectLabelVariantProps) {
    return (
        <LabelsWrapper>
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
