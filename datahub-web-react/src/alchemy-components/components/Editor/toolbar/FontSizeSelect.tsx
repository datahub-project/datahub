import { useActive, useCommands } from '@remirror/react';
import React from 'react';

import { SimpleSelect } from '@components/components/Select';

const FONT_SIZES = ['12px', '14px', '16px', '18px', '24px', '32px'];

export const FontSizeSelect = () => {
    const commands = useCommands();
    const active = useActive();

    const currentSize = FONT_SIZES.find((size) => active.fontSize({ size })) || '14px';

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
