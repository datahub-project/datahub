/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
