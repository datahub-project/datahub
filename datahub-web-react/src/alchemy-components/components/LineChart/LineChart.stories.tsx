import React from 'react';
import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import { LineChart } from './LineChart';
import { getMockedProps } from '../BarChart/utils';
import { DEFAULT_MAX_DOMAIN_VALUE } from '../BarChart/hooks/usePrepareScales';

const meta = {
    title: 'Charts / LineChart',
    component: LineChart,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.EXPERIMENTAL],
        docs: {
            subtitle: 'A component that is used to show LineChart',
        },
    },

    // Component-level argTypes
    argTypes: {
        data: {
            description: 'Array of datum to show',
        },
        maxYDomainForZeroData: {
            description:
                'For the case where the data has only zero values, you can set the yScale domain to better display the chart',
            table: {
                defaultValue: { summary: `${DEFAULT_MAX_DOMAIN_VALUE}` },
            },
        },
        popoverRenderer: {
            description: 'A function to replace default rendering of toolbar',
        },
        margin: {
            description: 'Add margins to chart',
        },
        lineColor: {
            description: 'Color of line on chart',
            control: {
                type: 'color',
            },
        },
        areaColor: {
            description: 'Color of area under line',
            control: {
                type: 'color',
            },
        },
        leftAxisProps: {
            description: 'The props for the left axis',
        },
        showLeftAxisLine: {
            description: 'Enable to show left vertical line',
        },
        bottomAxisProps: {
            description: 'The props for the bottom axis',
        },
        showBottomAxisLine: {
            description: 'Enable to show bottom horizontal line',
        },
        gridProps: {
            description: 'The props for the grid',
        },
        renderGradients: {
            description: 'A function to render different gradients that can be used as colors',
        },
        toolbarVerticalCrosshairStyle: {
            description: "Styles of toolbar's vertical line",
        },
        renderTooltipGlyph: {
            description: 'A function to render a glyph',
        },
        showGlyphOnSingleDataPoint: {
            description: 'Whether to show the glyph when there is only one data point',
            control: {
                type: 'boolean',
            },
        },
        renderGlyphOnSingleDataPoint: {
            description: 'A function to render a glyph for a single data point',
        },
    },

    // Define defaults
    args: {
        ...getMockedProps(),
        popoverRenderer: (datum) => <>DATUM: {JSON.stringify(datum)}</>,
        yScale: { type: 'linear', round: true, clamp: true },
    },
} satisfies Meta<typeof LineChart>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => (
        <div style={{ width: '900px', height: '350px' }}>
            <LineChart {...props} />
        </div>
    ),
};
