import { useActive, useCommands } from '@remirror/react';
import React from 'react';

import { SimpleSelect } from '@components/components/Select';

const OPTIONS = [
    { label: 'Heading 1', value: 1 },
    { label: 'Heading 2', value: 2 },
    { label: 'Heading 3', value: 3 },
    { label: 'Heading 4', value: 4 },
    { label: 'Heading 5', value: 5 },
    { label: 'Normal', value: 0 },
];

const DEFAULT_VALUE = 0;

export const HeadingMenu = () => {
    const commands = useCommands();
    const active = useActive(true);

    const activeHeading =
        OPTIONS.map(({ value }) => value).filter((level) => active.heading({ level }))?.[0] || DEFAULT_VALUE;

    const options = OPTIONS.map((option) => ({
        value: `${option.value}`,
        label: option.label,
    }));

    return (
        <SimpleSelect
            values={[`${activeHeading}`]}
            onUpdate={(values) => {
                const value = values[0];
                const level = +`${value}`;
                if (level) {
                    commands.toggleHeading({ level });
                } else {
                    commands.toggleHeading();
                }
                commands.focus();
            }}
            options={options}
            width="fit-content"
            showClear={false}
        />
    );
};
