import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import { CollapsiblePanel } from '@components/components/CollapsiblePanel/CollapsiblePanel';
import { Text } from '@components/components/Text';

const meta = {
    title: 'Components/CollapsiblePanel',
    component: CollapsiblePanel,
    parameters: {
        layout: 'centered',
    },
    argTypes: {
        header: {
            description: 'Header content displayed in the panel title',
            control: { type: 'text' },
        },
        children: {
            description: 'Body content displayed in the collapsible section',
            control: { type: 'text' },
        },
        defaultOpen: {
            description: 'Whether the panel should be open by default',
            control: { type: 'boolean' },
        },
        dataTestId: {
            description: 'Optional test ID for testing',
            control: { type: 'text' },
        },
    },
    args: {
        header: 'Panel Header',
        children: 'This is the panel content',
        defaultOpen: false,
    },
    tags: ['autodocs'],
} satisfies Meta<typeof CollapsiblePanel>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
    args: {} as any,
    render: (args: any) => <CollapsiblePanel {...args} />,
};

export const DefaultOpen: Story = {
    args: {
        defaultOpen: true,
    } as any,
    render: (args: any) => <CollapsiblePanel {...args} />,
};

export const WithLongContent: Story = {
    args: {
        header: 'Configuration Settings',
        children: (
            <div>
                <Text>
                    This panel contains longer content to demonstrate how the component handles different amounts of
                    text. The panel expands and collapses smoothly with a rotation animation on the toggle icon.
                </Text>
                <br />
                <Text color="textSecondary" size="sm">
                    You can put any React content here, including forms, lists, and other components.
                </Text>
            </div>
        ),
    } as any,
    render: (args: any) => <CollapsiblePanel {...args} />,
};

export const WithComplexHeader: Story = {
    args: {
        header: (
            <div style={{ display: 'flex', justifyContent: 'space-between', width: '100%', paddingRight: '8px' }}>
                <Text weight="semiBold">Rules Section</Text>
                <Text size="sm" color="textSecondary">
                    (5 items)
                </Text>
            </div>
        ),
        children: <Text>Manage your rules and conditions here</Text>,
    } as any,
    render: (args: any) => <CollapsiblePanel {...args} />,
};

export const Disabled: Story = {
    args: {
        header: 'Disabled Panel (Visual State)',
        children: <Text color="textSecondary">This panel is disabled</Text>,
    } as any,
    render: (args: any) => (
        <div style={{ pointerEvents: 'none', opacity: 0.5 }}>
            <CollapsiblePanel {...args} />
        </div>
    ),
};
