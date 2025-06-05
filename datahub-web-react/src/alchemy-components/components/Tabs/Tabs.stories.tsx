import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import { Tabs } from '@components/components/Tabs/Tabs';

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

const urlAwareTabs = [
    {
        key: 'overview',
        name: 'Overview',
        tooltip: 'View the overview',
        component: <div>This is the overview content</div>,
    },
    {
        key: 'details',
        name: 'Details',
        tooltip: 'View the details',
        component: <div>This is the details content</div>,
    },
    {
        key: 'settings',
        name: 'Settings',
        tooltip: 'View the settings',
        component: <div>This is the settings content</div>,
    },
];

const urlMap = {
    overview: '/overview',
    details: '/details',
    settings: '/settings',
};

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
        urlMap: {
            description:
                'A mapping of tab keys to URLs. When provided, the component will sync tab selection with the URL',
        },
        onUrlChange: {
            description: 'A custom handler for URL changes. Defaults to window.history.replaceState',
        },
        defaultTab: {
            description: 'The default tab to select when the URL does not match any tab',
        },
        getCurrentUrl: {
            description: 'A custom function to get the current URL. Defaults to window.location.pathname',
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

const UrlAwareTabsDemo = () => {
    const [selectedTab, setSelectedTab] = React.useState('overview');

    return (
        <div style={{ width: 225 }}>
            <div style={{ marginBottom: '1rem' }}>
                <p>Current URL: {window.location.pathname}</p>
                <p>Selected Tab: {selectedTab}</p>
            </div>
            <Tabs
                tabs={urlAwareTabs}
                selectedTab={selectedTab}
                onChange={setSelectedTab}
                urlMap={urlMap}
                defaultTab="overview"
            />
        </div>
    );
};

export const urlAware: Story = {
    tags: ['dev'],
    render: () => <UrlAwareTabsDemo />,
};
