/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
