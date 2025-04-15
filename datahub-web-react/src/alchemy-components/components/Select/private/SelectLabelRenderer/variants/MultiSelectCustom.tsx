import React from 'react';
import { Pill } from '@src/alchemy-components/components/Pills';
import { LabelsWrapper, Placeholder } from '../../../components';
import { SelectLabelVariantProps, SelectOption } from '../../../types';

export default function MultiSelectCustom<OptionType extends SelectOption>({
    selectedOptions,
    selectedValues,
    disabledValues,
    removeOption,
    placeholder,
    isMultiSelect,
    renderCustomSelectedValue,
}: SelectLabelVariantProps<OptionType>) {
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
