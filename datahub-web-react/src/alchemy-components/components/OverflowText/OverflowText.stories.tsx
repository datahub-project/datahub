import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import { OverflowText } from '@components/components/OverflowText/OverflowText';

// Auto Docs
const meta = {
    title: 'Typography / Overflow Text',
    component: OverflowText,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle:
                'Used to handle truncating text, adding and ellipses, and showing a tooltip if the text is truncated. This is based on the parent width.',
        },
    },

    // Component-level argTypes
    argTypes: {
        text: {
            description:
                'The text that will be truncated and shows an ellipses and tooltip if there is overflow based on the parent',
        },
    },

    // Define default args
    args: {
        text: 'FirstName LongLastNameToDemo',
    },
} satisfies Meta<typeof OverflowText>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => (
        <div style={{ width: 100 }}>
            <OverflowText {...props} />
        </div>
    ),
};
