import React from 'react';
import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import { BarChart } from './BarChart';
import { getMockedProps } from './utils';
import { DEFAULT_MIN_VALUE } from './hooks/useAdaptYAccessorToZeroValues';
import { DEFAULT_MAX_DOMAIN_VALUE } from './hooks/useAdaptYScaleToZeroValues';

const meta = {
    title: 'Charts / BarChart',
    component: BarChart,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.EXPERIMENTAL],
        docs: {
            subtitle: 'A component that is used to show BarChart',
        },
    },

    // Component-level argTypes
    argTypes: {
        data: {
            description: 'Array of datum to show',
        },
        xAccessor: {
            description: 'A function to convert datum to value of X',
        },
        yAccessor: {
            description: 'A function to convert datum to value of Y',
        },
        maxYDomainForZeroData: {
            description:
                'For the case where the data has only zero values, you can set the yScale domain to better display the chart',
            table: {
                defaultValue: { summary: `${DEFAULT_MAX_DOMAIN_VALUE}` },
            },
        },
        minYForZeroData: {
            description:
                'For the case where the data has only zero values, you can set minimal Y value to better display the chart',
            table: {
                defaultValue: { summary: `${DEFAULT_MIN_VALUE}` },
            },
        },
        popoverRenderer: {
            description: 'A function to replace default rendering of toolbar',
        },
        margin: {
            description: 'Add margins to chart',
        },
        barColor: {
            description: 'Color of bar',
            control: {
                type: 'color',
            },
        },
        barSelectedColor: {
            description: 'Color of selected bar',
            control: {
                type: 'color',
            },
        },
        leftAxisProps: {
            description: 'The props for the left axis',
        },
        bottomAxisProps: {
            description: 'The props for the bottom axis',
        },
        gridProps: {
            description: 'The props for the grid',
        },
        renderGradients: {
            description: 'A function to render different gradients that can be used as colors',
        },
    },

    // Define defaults
    args: {
        ...getMockedProps(),
        popoverRenderer: (datum) => <>DATUM: {JSON.stringify(datum)}</>,
    },
} satisfies Meta<typeof BarChart>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => (
        <div style={{ width: '900px', height: '350px' }}>
            <BarChart {...props} />
        </div>
    ),
};
