import React from 'react';
import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import { GraphCard } from './GraphCard';
import { CalendarChart } from '../CalendarChart';
import { getMockedProps } from '../CalendarChart/utils';
import { Button } from '../Button';
import { SimpleSelect } from '../Select';

const meta = {
    title: 'Charts / GraphCard',
    component: GraphCard,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.EXPERIMENTAL],
        docs: {
            subtitle: 'A card component that wraps graphs',
        },
    },

    // Component-level argTypes
    argTypes: {
        title: {
            description: 'The title of the card',
        },
        subTitle: {
            description: 'The subtitle of the card',
        },
        isEmpty: {
            description: 'Enable when there are no data to show in graph to show the empty message',
            control: 'boolean',
        },
        loading: {
            description: 'Show loading state',
            control: 'boolean',
        },
        renderGraph: {
            description: 'A function to render certain graph',
        },
        renderControls: {
            description: 'A function to render additiona controls on the card',
        },
    },

    // Define defaults
    args: {
        title: 'Title of the card',
        subTitle: 'Description of the card',
        renderControls: () => (
            <>
                <Button icon="Add" variant="outline" size="md">
                    Assertion
                </Button>
                <SimpleSelect
                    options={[
                        { value: 'Test1', label: 'Test1' },
                        { value: 'Test2', label: 'Test2' },
                        { value: 'Test3', label: 'Test3' },
                    ]}
                />
                <SimpleSelect
                    values={['year']}
                    options={[
                        { value: 'year', label: 'Year' },
                        { value: 'Test2', label: 'Test2' },
                        { value: 'Test3', label: 'Test3' },
                    ]}
                />
            </>
        ),
        renderGraph: () => (
            <CalendarChart {...getMockedProps()} popoverRenderer={(day) => <>{JSON.stringify(day)}</>} />
        ),
        graphHeight: 'fit-content',
    },
} satisfies Meta<typeof GraphCard>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => (
        <div style={{ width: '900px' }}>
            <GraphCard {...props} />
        </div>
    ),
};
