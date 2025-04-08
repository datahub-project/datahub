import React from 'react';

import { LabelsWrapper, Placeholder } from '@components/components/Select/components';
import { SelectLabelVariantProps } from '@components/components/Select/types';

import { Pill } from '@src/alchemy-components/components/Pills';

export default function MultiSelectCustom({
    selectedOptions,
    selectedValues,
    disabledValues,
    removeOption,
    placeholder,
    isMultiSelect,
    renderCustomSelectedValue,
}: SelectLabelVariantProps) {
    return (
        <LabelsWrapper shouldShowGap={selectedOptions.length > 1}>
            {!selectedValues.length && <Placeholder>{placeholder}</Placeholder>}
            {!!selectedOptions.length &&
                isMultiSelect &&
                selectedOptions.map((o) => {
                    const isDisabled = disabledValues?.includes(o.value);
                    return renderCustomSelectedValue ? (
                        renderCustomSelectedValue(o)
                    ) : (
                        <Pill
                            label={o?.label}
                            rightIcon={!isDisabled ? 'Close' : ''}
                            size="sm"
                            key={o?.value}
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
