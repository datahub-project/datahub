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
        scrollToTopOnChange: {
            description: 'Whether to scroll to the top of the tabs container when switching tabs',
            control: { type: 'boolean' },
        },
        maxHeight: {
            description:
                'Maximum height of the scrollable tabs container (only applies when scrollToTopOnChange is true)',
            control: { type: 'text' },
        },
        stickyHeader: {
            description: 'Whether to make the tab headers sticky when scrolling',
            control: { type: 'boolean' },
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

const StickyHeaderTabsDemo = () => {
    const [selectedTab, setSelectedTab] = React.useState('tab1');

    const stickyTabs = [
        {
            key: 'tab1',
            name: 'Tab One',
            component: (
                <div style={{ padding: '20px' }}>
                    <h3>Tab One with Sticky Header</h3>
                    {Array.from({ length: 40 }, (_, i) => (
                        <p key={i}>
                            This is paragraph {i + 1}. The tab header will stick to the top when you scroll down. Lorem
                            ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut
                            labore et dolore magna aliqua.
                        </p>
                    ))}
                </div>
            ),
        },
        {
            key: 'tab2',
            name: 'Tab Two',
            component: (
                <div style={{ padding: '20px' }}>
                    <h3>Tab Two Content</h3>
                    {Array.from({ length: 35 }, (_, i) => (
                        <div key={i} style={{ marginBottom: '15px', padding: '15px', backgroundColor: '#f0f0f0' }}>
                            Content block {i + 1} - Notice how the tab header remains visible at the top while
                            scrolling.
                        </div>
                    ))}
                </div>
            ),
        },
        {
            key: 'tab3',
            name: 'Tab Three',
            component: (
                <div style={{ padding: '20px' }}>
                    <h3>Tab Three Content</h3>
                    {Array.from({ length: 50 }, (_, i) => (
                        <p key={i}>
                            Line {i + 1}: The sticky header allows you to easily switch between tabs even when scrolled
                            deep into the content. This is particularly useful for long content like logs or detailed
                            configuration files.
                        </p>
                    ))}
                </div>
            ),
        },
    ];

    return (
        <div style={{ width: '700px', height: '500px' }}>
            <h4>Sticky Header & Scroll to Top on Tab Change Demo</h4>
            <p>Scroll down within the tab content to see the sticky header behavior and switch between tabs</p>
            <Tabs
                tabs={stickyTabs}
                selectedTab={selectedTab}
                onChange={setSelectedTab}
                scrollToTopOnChange
                maxHeight="400px"
                stickyHeader
            />
        </div>
    );
};

export const stickyHeader: Story = {
    tags: ['dev'],
    render: () => <StickyHeaderTabsDemo />,
};
