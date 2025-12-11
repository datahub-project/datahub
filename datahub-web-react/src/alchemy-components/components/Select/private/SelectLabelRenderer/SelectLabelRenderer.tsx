/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

import MultiSelectCustom from '@components/components/Select/private/SelectLabelRenderer/variants/MultiSelectCustom';
import MultiSelectDefault from '@components/components/Select/private/SelectLabelRenderer/variants/MultiSelectDefault';
import MultiSelectLabeled from '@components/components/Select/private/SelectLabelRenderer/variants/MultiSelectLabeled';
import SingleSelectCustom from '@components/components/Select/private/SelectLabelRenderer/variants/SingleSelectCustom';
import SingleSelectDefault from '@components/components/Select/private/SelectLabelRenderer/variants/SingleSelectDefault';
import SingleSelectLabeled from '@components/components/Select/private/SelectLabelRenderer/variants/SingleSelectLabeled';
import { SelectLabelDisplayProps, SelectOption } from '@components/components/Select/types';

export default function SelectLabelRenderer<OptionType extends SelectOption>({
    variant,
    ...props
}: SelectLabelDisplayProps<OptionType>) {
    const { isMultiSelect, options, selectedValues } = props;

    const selectedOptions = useMemo(
        () => options.filter((opt) => selectedValues.includes(opt.value)),
        [options, selectedValues],
    );

    const getComponent = () => {
        if (isMultiSelect) {
            switch (variant) {
                case 'labeled':
                    return MultiSelectLabeled;
                case 'custom':
                    return MultiSelectCustom;
                default:
                    return MultiSelectDefault;
            }
        }

        switch (variant) {
            case 'labeled':
                return SingleSelectLabeled;
            case 'custom':
                return SingleSelectCustom;
            default:
                return SingleSelectDefault;
        }
    };

    return getComponent()({ ...props, selectedOptions });
}
