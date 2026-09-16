import { useCommands, useHelpers } from '@remirror/react';
import React from 'react';

import { ptToPx } from '@components/components/Editor/utils';
import { SimpleSelect } from '@components/components/Select';

const FONT_SIZES = ['12px', '14px', '16px', '18px', '24px', '32px'];

export const FontSizeSelect = () => {
    const commands = useCommands();

    const getFontSize = useHelpers().getFontSizeForSelection;

    const currentSize = (() => {
        const sizeTuple = getFontSize()?.[0];
        if (!sizeTuple) return '14px';
        const [value, unit] = sizeTuple;
        if (value === 0) return '14px';
        if (unit === 'px') return `${value}px`;
        if (unit === 'pt') return `${ptToPx(value)}px`;
        return `${value}${unit}`;
    })();

    const options = FONT_SIZES.map((size) => ({
        value: size,
        label: size,
    }));

    const handleChange = (value: string) => {
        commands.setFontSize(value);
        commands.focus();
    };

    return (
        <SimpleSelect
            options={options}
            values={[currentSize]}
            onUpdate={(values) => {
                handleChange(values[0]);
            }}
            showClear={false}
            width="fit-content"
        />
    );
};
