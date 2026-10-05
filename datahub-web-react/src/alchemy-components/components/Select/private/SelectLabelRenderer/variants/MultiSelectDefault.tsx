import { X } from '@phosphor-icons/react/dist/csr/X';
import React from 'react';
import { useTheme } from 'styled-components';

import { LabelsWrapper, Placeholder } from '@components/components/Select/components';
import { SelectLabelVariantProps, SelectOption } from '@components/components/Select/types';

import { Pill } from '@src/alchemy-components/components/Pills';

export default function MultiSelectDefault<OptionType extends SelectOption>({
    selectedOptions,
    selectedValues,
    disabledValues,
    removeOption,
    placeholder,
    isMultiSelect,
}: SelectLabelVariantProps<OptionType>) {
    const theme = useTheme();
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
                            rightIcon={!isDisabled ? X : undefined}
                            size="sm"
                            key={o.value}
                            customIconRenderer={o.icon ? () => o.icon : undefined}
                            // The default gray/filled Pill text resolves to `textSecondary`, which is
                            // noticeably lower-contrast than the rest of the select's selected-value text
                            // (`text`). Bump it to match so selected chips stay legible.
                            customStyle={{ color: theme.colors.text }}
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
