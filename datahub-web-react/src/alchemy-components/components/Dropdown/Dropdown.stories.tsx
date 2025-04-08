import { BADGE } from '@geometricpanda/storybook-addon-badges';
import { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import Dropdown from '@components/components/Dropdown/Dropdown';

// Auto Docs
const meta = {
    title: 'Components / Dropdown',
    component: Dropdown,

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
        open: {
            description: 'Controlled open state of dropdown',
        },
        dropdownRender: {
            description: "Rendering function of dropdown's content",
        },
        disabled: {
            description: 'Set to true to disable the dropdown',
        },
        overlayClassName: {
            description: 'Class name of the dropdown',
        },
        onOpenChange: {
            description: 'Called when dropdown opens/closes',
        },
        placement: {
            description: 'Placement of the dropdown',
            table: {
                type: {
                    summary:
                        '["topLeft", "topCenter", "topRight", "bottomLeft", "bottomCenter", "bottomRight", "top", "bottom"]',
                },
            },
        },
    },

    // Define defaults
    args: {
        dropdownRender: () => <div>Test content</div>,
    },
} satisfies Meta<typeof Dropdown>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook & is used as the code sandbox
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => (
        <Dropdown {...props}>
            <button type="button">Click</button>
        </Dropdown>
    ),
};
