import { useActive, useCommands } from '@remirror/react';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { SimpleSelect } from '@components/components/Select';

const HEADING_LEVELS = [1, 2, 3, 4, 5];
const DEFAULT_VALUE = 0;

export const HeadingMenu = () => {
    const { t } = useTranslation('alchemy');
    const commands = useCommands();
    const active = useActive(true);

    const activeHeading = HEADING_LEVELS.filter((level) => active.heading({ level }))?.[0] || DEFAULT_VALUE;

    const options = [
        ...HEADING_LEVELS.map((level) => ({ value: `${level}`, label: t(`editor.heading.h${level}`) })),
        { value: `${DEFAULT_VALUE}`, label: t('editor.heading.normal') },
    ];

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
