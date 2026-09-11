import type { Meta, StoryObj } from '@storybook/react';
import React, { useState } from 'react';

import { MultiSelectInput } from '@components/components/MultiSelectInput/MultiSelectInput';

const meta = {
    title: 'Forms / MultiSelectInput',
    component: MultiSelectInput,
    parameters: {
        layout: 'centered',
    },
    argTypes: {
        label: {
            description: 'Label displayed above the input',
            control: { type: 'text' },
        },
        placeholder: {
            description: 'Placeholder text shown when empty',
            control: { type: 'text' },
        },
        values: {
            description: 'Array of selected tag values',
            control: { type: 'object' },
        },
        disabled: {
            description: 'Whether the input is disabled',
            control: { type: 'boolean' },
        },
        error: {
            description: 'Error message to display',
            control: { type: 'text' },
        },
        helperText: {
            description: 'Helper text below the input',
            control: { type: 'text' },
        },
        width: {
            description: 'Width of the input (number for pixels or string like "100%")',
            control: { type: 'text' },
        },
        id: {
            description: 'HTML id attribute',
            control: { type: 'text' },
        },
        className: {
            description: 'CSS class name',
            control: { type: 'text' },
        },
        inputTestId: {
            description: 'Test ID for the input element',
            control: { type: 'text' },
        },
    },
    args: {
        placeholder: 'Enter tags...',
        label: 'Tags',
        values: [],
        disabled: false,
    },
    tags: ['autodocs'],
} satisfies Meta<typeof MultiSelectInput>;

export default meta;
type Story = StoryObj<typeof meta>;

// Wrapper component for managing state in stories
const MultiSelectInputWithState = (args: any) => {
    const [values, setValues] = useState(args.values || []);
    return <MultiSelectInput {...args} values={values} onUpdate={setValues} />;
};

export const Default: Story = {
    args: {} as any,
    render: (args: any) => <MultiSelectInputWithState {...args} />,
};

export const WithValues: Story = {
    args: {
        values: ['React', 'TypeScript', 'Storybook'],
    } as any,
    render: (args: any) => <MultiSelectInputWithState {...args} />,
};

export const WithHelperText: Story = {
    args: {
        helperText: 'Press Enter or comma to add a tag',
    } as any,
    render: (args: any) => <MultiSelectInputWithState {...args} />,
};

export const WithError: Story = {
    args: {
        error: 'Please enter at least one tag',
    } as any,
    render: (args: any) => <MultiSelectInputWithState {...args} />,
};

export const Disabled: Story = {
    args: {
        disabled: true,
        values: ['React', 'TypeScript'],
    } as any,
    render: (args: any) => <MultiSelectInputWithState {...args} />,
};

export const CustomWidth: Story = {
    args: {
        width: 500,
    } as any,
    render: (args: any) => <MultiSelectInputWithState {...args} />,
};

export const NoLabel: Story = {
    args: {
        label: undefined,
    } as any,
    render: (args: any) => <MultiSelectInputWithState {...args} />,
};

export const MultipleTags: Story = {
    args: {
        label: 'Multiple Tags',
        values: ['Component', 'Input', 'Selection', 'Tags', 'UI', 'React'],
    } as any,
    render: (args: any) => <MultiSelectInputWithState {...args} />,
};
