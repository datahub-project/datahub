import React from 'react';
<<<<<<< HEAD

import { LabelsWrapper, Placeholder } from '@components/components/Select/components';
import { SelectLabelVariantProps, SelectOption } from '@components/components/Select/types';

import { Pill } from '@src/alchemy-components/components/Pills';

=======
import { Pill } from '@src/alchemy-components/components/Pills';
import { LabelsWrapper, Placeholder } from '../../../components';
import { SelectLabelVariantProps, SelectOption } from '../../../types';

>>>>>>> dbad52283b070c7cc136306c1553770db2f72105
export default function MultiSelectDefault<OptionType extends SelectOption>({
    selectedOptions,
    selectedValues,
    disabledValues,
    removeOption,
    placeholder,
    isMultiSelect,
}: SelectLabelVariantProps<OptionType>) {
    return (
        <LabelsWrapper shouldShowGap={selectedOptions.length > 1}>
            {!selectedValues.length && <Placeholder>{placeholder}</Placeholder>}
            {!!selectedOptions.length &&
                isMultiSelect &&
                selectedOptions.map((o) => {
                    const isDisabled = disabledValues?.includes(o.value);
                    return (
                        <Pill
                            label={o.label}
                            rightIcon={!isDisabled ? 'Close' : ''}
                            size="sm"
                            key={o.value}
                            onClickRightIcon={(e) => {
                                e.stopPropagation();
                                removeOption?.(o);
                            }}
                            clickable={!isDisabled}
                        />
                    );
                })}
        </LabelsWrapper>
    );
}
