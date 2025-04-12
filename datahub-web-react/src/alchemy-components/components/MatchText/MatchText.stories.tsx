import React from 'react';

import type { Meta, StoryObj } from '@storybook/react';
import { BADGE } from '@geometricpanda/storybook-addon-badges';

import MatchText from './MatchText';
import { matchTextDefaults } from './defaults';

// Auto Docs
const meta = {
    title: 'Typography / MatchText',
    component: MatchText,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'Used to highlight text parts dynamically',
        },
    },

    // Component-level argTypes
    argTypes: {
        text: {
            description: 'The initial text to matching',
            table: {
                type: {
                    summary: 'string',
                },
            },
        },
        highlight: {
            description: 'The text to highlight',
            table: {
                type: {
                    summary: 'string',
                },
            },
        },
        type: {
            description: 'The type of text to display.',
            table: {
                defaultValue: { summary: matchTextDefaults.type },
            },
        },
        size: {
            description: 'Override the size of the text.',
            table: {
                defaultValue: { summary: `${matchTextDefaults.size}` },
            },
        },
        color: {
            description: 'Override the color of the text.',
            table: {
                defaultValue: { summary: matchTextDefaults.color },
            },
        },
        weight: {
            description: 'Override the weight of the text.',
            table: {
                defaultValue: { summary: matchTextDefaults.weight },
            },
        },
        highlightedTextProps: {
            description: 'Overide text props for highlighted parts',
            table: {
                defaultValue: { summary: JSON.stringify(matchTextDefaults.highlightedTextProps) },
            },
        },
    },

    // Define default args
    args: {
        text: `Lorem ipsum dolor sit amet, consectetur adipiscing elit. 
Maecenas aliquet nulla id felis vehicula, et posuere dui dapibus. 
Nullam rhoncus massa non tortor convallis, in blandit turpis rutrum. 
Morbi tempus velit mauris, at mattis metus mattis sed. Nunc molestie efficitur lectus, vel mollis eros.`,
        highlight: 'ipsum',
        type: matchTextDefaults.type,
        size: matchTextDefaults.size,
        color: matchTextDefaults.color,
        weight: matchTextDefaults.weight,
        highlightedTextProps: matchTextDefaults.highlightedTextProps,
    },
} satisfies Meta<typeof MatchText>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <MatchText {...props} />,
};
