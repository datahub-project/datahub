import type { Meta, StoryObj } from '@storybook/react';
import moment from 'moment';
import React, { useState } from 'react';
import { DatePicker, datePickerDefault } from './DatePicker';
import { DatePickerValue } from './types';

const meta = {
    title: 'Forms / DatePicker',
    component: DatePicker,
    parameters: {
        layout: 'centered',
        docs: {
            subtitle: 'A component to select a date.',
        },
    },
    argTypes: {
        value: {
            description: 'The value of the component can be a Moment object or null | undefined',
            table: {
                defaultValue: { summary: 'undefined' },
                type: { summary: 'Moment | null | undefined' },
            },
        },
        onChange: {
            description: 'Callback function, can be executed when the selected date is changing',
            table: {
                defaultValue: { summary: 'undefined' },
            },
        },
        disabled: {
            description: 'Determine whether the DatePicker is disabled',
            table: {
                defaultValue: { summary: 'false' },
            },
            control: {
                type: 'boolean',
            },
        },
        variant: {
            description: 'Preset of predefined props',
            table: {
                defaultValue: { summary: 'DEFAULT' },
            },
        },
    },
    args: { ...datePickerDefault },
} satisfies Meta<typeof DatePicker>;

export default meta;

type Story = StoryObj<typeof meta>;

function WrappedDatePicker(props) {
    const [value, setValue] = useState<DatePickerValue>(moment());
    return <DatePicker value={value} onChange={(v) => setValue(v)} {...props} />;
}

export const sandbox: Story = {
    tags: ['dev'],
    render: WrappedDatePicker,
};
