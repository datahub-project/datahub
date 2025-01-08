import React from 'react';
import { Pill } from '@src/alchemy-components/components/Pills';
import { LabelsWrapper, Placeholder } from '../../../components';
import { SelectLabelVariantProps } from '../../../types';

export default function MultiSelectDefault({
    selectedOptions,
    selectedValues,
    disabledValues,
    removeOption,
    placeholder,
    isMultiSelect,
}: SelectLabelVariantProps) {
    return (
        <LabelsWrapper>
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
