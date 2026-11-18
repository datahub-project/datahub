import type { Meta, StoryObj } from '@storybook/react';
import React, { useEffect, useState } from 'react';

import { CronBuilder } from '@components/components/CronBuilder';
import { CronBuilderProps } from '@components/components/CronBuilder/types';

function ControlledCronBuilder(props: Omit<CronBuilderProps, 'setValue'>) {
    const [value, setValue] = useState(props.value ?? '');
    useEffect(() => setValue(props.value ?? ''), [props.value]);
    return <CronBuilder {...props} value={value} setValue={setValue} />;
}

const meta = {
    title: 'Forms / CronBuilder',
    component: ControlledCronBuilder,
    parameters: {
        layout: 'centered',
        docs: {
            subtitle: 'A themed cron expression editor built on react-js-cron.',
        },
    },
    tags: ['autodocs'],
    argTypes: {
        value: {
            description: 'Cron expression',
            control: 'text',
        },
        clearButton: {
            description: 'Whether to show the clear button',
            control: 'boolean',
        },
        leadingZero: {
            description: 'Whether to display numbers with a leading zero',
            control: 'boolean',
        },
        disabled: {
            description: 'Whether the editor is disabled',
            control: 'boolean',
        },
    },
    args: {
        value: '0 0 * * *',
        clearButton: false,
        leadingZero: true,
        disabled: false,
    },
} satisfies Meta<typeof ControlledCronBuilder>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Basic: Story = {};

export const Disabled: Story = {
    args: {
        disabled: true,
    },
};
