import React from 'react';
import { Meta, StoryObj } from '@storybook/react';
import { IncidentPriorityLabel } from './IncidentPriorityLabel';

const meta: Meta<typeof IncidentPriorityLabel> = {
    title: 'Components / IncidentPriorityLabel',
    component: IncidentPriorityLabel,

    // Component-level parameters
    parameters: {
        layout: 'centered',
        docs: {
            subtitle: 'Displays a priority label for incidents with an icon and title.',
        },
    },

    // Component-level argTypes
    argTypes: {
        priority: {
            description: 'Incident string containing priority',
            control: { type: 'text' },
            table: {
                type: { summary: '"CRITICAL" | "HIGH" | "MEDIUM" | "LOW"' },
            },
        },
        title: {
            description: 'Incident string containing title',
            control: { type: 'text' },
            table: {
                type: { summary: 'title: string' },
            },
        },
    },

    // Default props
    args: {
        priority: 'CRITICAL',
        title: 'Critical',
    },
};

export default meta;

type Story = StoryObj<typeof meta>;

// Sandbox Story
export const sandbox: Story = {
    render: (props) => <IncidentPriorityLabel {...props} />,
};

// Example Stories
export const criticalPriority: Story = {
    args: {
        priority: 'CRITICAL',
        title: 'Critical',
    },
};

export const highPriority: Story = {
    args: {
        priority: 'HIGH',
        title: 'High',
    },
};

export const mediumPriority: Story = {
    args: {
        priority: 'MEDIUM',
        title: 'Medium',
    },
};

export const lowPriority: Story = {
    args: {
        priority: 'LOW',
        title: 'Low',
    },
};

export const unknownPriority: Story = {
    args: {
        priority: 'UNKNOWN',
        title: 'Unknown',
    },
};
