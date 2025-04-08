import { BADGE } from '@geometricpanda/storybook-addon-badges';
import { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import AutoComplete from '@components/components/AutoComplete/AutoComplete';

// Auto Docs
const meta = {
    title: 'Components / AutoComplete',
    component: AutoComplete,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'This component allows to add autocompletion',
        },
    },

    // Component-level argTypes
    argTypes: {
        dataTestId: {
            description: 'Optional property to set data-testid',
            control: 'text',
        },
        className: {
            description: 'Optional class names to pass into AutoComplete',
            control: 'text',
        },
        value: {
            description: 'Value of input',
        },
        defaultValue: {
            description: 'Selected option by default',
        },
        options: {
            description: 'Options available in dropdown',
            table: {
                type: {
                    summary: 'OptionType',
                    detail: `{
    label: React.ReactNode;
    value?: string | number | null;
    disabled?: boolean;
    [name: string]: any;
    children?: Omit<OptionType, 'children'>[];
}
                    `,
                },
            },
        },
        open: {
            description: 'Controlled open state of dropdown',
        },
        defaultActiveFirstOption: {
            description: 'Whether active first option by default',
        },
        filterOption: {
            description: 'If true, filter options by input, if function, filter options against it',
        },
        dropdownContentHeight: {
            description: "Height of dropdown's content",
        },
        onSelect: {
            description: 'Called when a option is selected',
        },
        onSearch: {
            description: 'Called when searching items',
        },
        onChange: {
            description: 'Called when selecting an option or changing an input value',
        },
        onDropdownVisibleChange: {
            description: 'Called when dropdown opened/closed',
        },
        onClear: {
            description: 'Called when clear',
        },
        dropdownRender: {
            description: 'Customize dropdown content',
        },
        dropdownAlign: {
            description: "Adjust how the autocomplete's dropdown should be aligned",
        },
        style: {
            description: 'Additional styles for the wrapper of the children',
        },
        dropdownStyle: {
            description: 'Additional styles for the dropdown',
        },
        dropdownMatchSelectWidth: {
            description:
                'Determine whether the dropdown menu and the select input are the same width.' +
                'Default set min-width same as input. Will ignore when value less than select width.',
        },
    },

    // Define defaults
    args: {
        options: [
            { label: 'test', value: 'test' },
            { label: 'test2', value: 'test2' },
        ],
    },
} satisfies Meta<typeof AutoComplete>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook & is used as the code sandbox
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => (
        <AutoComplete {...props}>
            <input />
        </AutoComplete>
    ),
};
