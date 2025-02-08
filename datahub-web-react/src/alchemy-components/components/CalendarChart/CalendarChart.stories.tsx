import React from 'react';
import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import { CalendarChart } from './CalendarChart';
import { getMockedProps } from './utils';

const meta = {
    title: 'Charts / CalendarChart',
    component: CalendarChart,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.EXPERIMENTAL],
        docs: {
            subtitle: 'A component that is used to show CalendarChart',
        },
    },

    // Component-level argTypes
    argTypes: {
        data: {
            description: 'Array of datum to show',
        },
        startDate: {
            description: 'The first day of calendar (it will be rounded by weeks)',
        },
        endDate: {
            description: 'The last day of calendar (it will be rounded by weeks)',
        },
        colorAccessor: {
            description: "A function to get color by datum's value",
        },
        showPopover: {
            description: 'Enable to add popovers',
            table: {
                defaultValue: { summary: 'true' },
            },
            control: {
                type: 'boolean',
            },
        },
        popoverRenderer: {
            description: "A function to render popover's content of datum's value",
        },
        leftAxisLabelProps: {
            description: 'Props for label of left axis',
        },
        showLeftAxisLine: {
            description: 'Enable to show left vertical line',
        },
        bottomAxisLabelProps: {
            description: 'Props for label of bottom axis',
        },
        margin: {
            description: 'Margins for CalendarChart',
        },
        selectedDay: {
            description: 'Set a date in format `YYYY-MM-DD` to select this day on calendar',
            control: {
                type: 'text',
            },
        },
    },

    // Define defaults
    args: {
        ...getMockedProps(),
        popoverRenderer: (day) => <>{JSON.stringify(day)}</>,
        onDayClick: () => null,
    },
} satisfies Meta<typeof CalendarChart>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => (
        <div style={{ width: '900px', height: '350px' }}>
            <CalendarChart {...props} />
        </div>
    ),
};
