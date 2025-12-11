/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
