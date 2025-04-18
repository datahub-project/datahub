import React from 'react';
import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import { BarChart } from './BarChart';
import { generateMockDataHorizontal, getMockedProps } from './utils';
import { DEFAULT_MIN_VALUE } from './hooks/usePrepareAccessors';
import { DEFAULT_MAX_DOMAIN_VALUE } from './hooks/usePrepareScales';
import { abbreviateNumber } from '../dataviz/utils';
import { DEFAULT_LENGTH_OF_LEFT_AXIS_LABEL } from './constants';

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
            table: {
                type: {
                    summary: 'DatumType[]',
                    detail:
                        'The DatumType includes:\n' +
                        '- x: A numeric value representing the x-coordinate.\n' +
                        '- y: A numeric value representing the y-coordinate.\n' +
                        '- colorScheme (optional): A ColorScheme enum value to define the color of the data point. ' +
                        'If not provided, a default color may be used.',
                },
            },
        },
        horizontal: {
            description: 'Whether to show horizontal bars',
            table: {
                defaultValue: { summary: 'false' },
            },
            control: {
                type: 'boolean',
            },
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
        leftAxisProps: {
            description: 'The props for the left axis',
        },
        maxLengthOfLeftAxisLabel: {
            description:
                'Enables truncating of label up to provided value. The full value will be available in popover',
            table: {
                defaultValue: { summary: `${DEFAULT_LENGTH_OF_LEFT_AXIS_LABEL}` },
                type: {
                    summary: 'number',
                },
            },
            control: {
                type: 'number',
            },
        },
        showLeftAxisLine: {
            description: 'Enable to show left vertical line',
        },
        bottomAxisProps: {
            description: 'The props for the bottom axis',
        },
        gridProps: {
            description: 'The props for the grid',
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

export const horizontal: Story = {
    args: {
        horizontal: true,
        xScale: {
            type: 'linear',
            nice: true,
            round: true,
        },
        yScale: {
            type: 'band',
            reverse: true,
            padding: 0.1,
        },
        gridProps: {
            rows: false,
            columns: true,
            strokeWidth: 1,
        },
        margin: {
            top: 0,
            right: 20,
            bottom: 0,
            left: 20,
        },

        bottomAxisProps: {
            tickFormat: (v) => abbreviateNumber(v),
        },
    },
    render: (props) => {
        const data = generateMockDataHorizontal();

        return (
            <div style={{ width: '900px', height: '300px' }}>
                <BarChart
                    {...props}
                    data={data}
                    leftAxisProps={{
                        tickFormat: (y) => data.find((datum) => datum.y === y)?.label,
                        computeNumTicks: () => data.length,
                    }}
                />
            </div>
        );
    },
};
