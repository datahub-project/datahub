import React from 'react';

import type { Meta, StoryObj } from '@storybook/react';
import { BADGE } from '@geometricpanda/storybook-addon-badges';

import WhiskerChart from './WhiskerChart';
import { DEFAULT_BOX_SIZE, DEFAULT_GAP_BETWEEN_WHISKERS } from './constants';

// Auto Docs
const meta = {
    title: 'Charts / WhiskerChart',
    component: WhiskerChart,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'Used to render text and paragraphs within an interface.',
        },
    },

    // Component-level argTypes
    argTypes: {
        data: {
            description:
                'An array of WhiskerDatum objects representing statistical data and optional color scheme for rendering whisker plots',
            table: {
                type: {
                    summary: 'WhiskerDatum[]',
                },
            },
        },
        boxSize: {
            description: 'Size of box',
            table: {
                defaultValue: { summary: `${DEFAULT_BOX_SIZE}` },
            },
            control: {
                type: 'number',
            },
        },
        gap: {
            description: 'Gap between whiskers',
            table: {
                defaultValue: { summary: `${DEFAULT_GAP_BETWEEN_WHISKERS}` },
            },
            control: {
                type: 'number',
            },
        },
        axisLabel: {
            description: 'Optional label of the axis',
            table: {
                defaultValue: { summary: 'undefined' },
                type: { summary: 'string' },
            },
            control: {
                type: 'text',
            },
        },
        renderTooltip: {
            description: 'Rendering function to customize tolltip',
        },
        renderWhisker: {
            description: 'Rendering function to customize whisker',
        },
    },

    // Define default args
    args: {
        data: [
            {
                key: 'test',
                min: 5,
                firstQuartile: 7,
                median: 18,
                thirdQuartile: 30,
                max: 50,
            },
            {
                key: 'test2',
                min: -10,
                firstQuartile: 12,
                median: 32,
                thirdQuartile: 45,
                max: 55,
                colorShemeSettings: {
                    box: 'red',
                    boxAlternative: 'blue',
                    medianLine: 'green',
                    alternative: 'black',
                },
            },
        ],
    },
} satisfies Meta<typeof WhiskerChart>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => (
        <div style={{ width: '900px', height: '350px' }}>
            <WhiskerChart {...props} />
        </div>
    ),
};
