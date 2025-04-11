import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';
import { Tabs } from './Tabs';

const exampleTabs = [
    {
        key: 'tab1',
        name: 'Tab One',
        tooltip: 'Tooltip for Tab One',
        component: <div>This is the content for Tab One</div>,
    },
    {
        key: 'tab2',
        name: 'Tab Two',
        tooltip: 'Tooltip for Tab Two',
        component: <div>This is the content for Tab Two</div>,
    },
    {
        key: 'tab3',
        name: 'Tab Three',
        tooltip: 'Tooltip for Tab Three',
        component: <div>This is the content for Tab Three</div>,
    },
];

// Auto Docs
const meta = {
    title: 'Components / Tabs',
    component: Tabs,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.BETA],
        docs: {
            subtitle: 'A component for rendering tabs where you can select and render content under them',
        },
    },

    // Component-level argTypes
    argTypes: {
        tabs: {
            description: 'The tabs you want to display',
        },
        selectedTab: {
            description: 'A controlled pieces of state for which tab is selected. This is the key of the tab',
        },
        onChange: {
            description: 'The handler called when any tab is clicked',
        },
    },

    // Args for the story

    args: {
        tabs: exampleTabs,
    },
} satisfies Meta<typeof Tabs>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => (
        <div style={{ width: 225 }}>
            <Tabs {...props} />
        </div>
    ),
};
