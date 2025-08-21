import { useActive, useCommands } from '@remirror/react';
import React from 'react';

import { ColorSwatch } from '@components/components/Editor/toolbar/ColorSwatch';
import { flattenColors } from '@components/components/Editor/utils';
import { SimpleSelect } from '@components/components/Select';
import { colors } from '@components/theme';

const colorsToExclude = ['transparent', 'currentColor', 'white'];

export const TextColorSelect = () => {
    const commands = useCommands();
    const active = useActive();
    const flattenedColors = flattenColors(colors).filter(({ key }) => !colorsToExclude.includes(key));

    const currentColor =
        flattenedColors.find(({ value }) => active.textColor({ color: value }))?.value || colors.gray[600];

    const options = flattenedColors.map((color) => ({
        value: color.value,
        label: color.value,
    }));

    const handleChange = (value: string) => {
        commands.setTextColor(value);
        commands.focus();
    };

    const customLabelRenderer = (option) => {
        return (
            <div style={{ display: 'flex', alignItems: 'center' }}>
                <ColorSwatch color={option.value} />
                <span>{option.label}</span>
            </div>
        );
    };

    return (
        <SimpleSelect
            options={options}
            values={[currentColor]}
            onUpdate={(values) => {
                handleChange(values[0]);
            }}
            showClear={false}
            width="fit-content"
            renderCustomOptionText={customLabelRenderer}
        />
    );
};
