import { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import { IncidentPriorityLabel } from '@components/components/IncidentPriorityLabel/IncidentPriorityLabel';

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
    },

    // Default props
    args: {
        priority: 'CRITICAL',
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
    },
};

export const highPriority: Story = {
    args: {
        priority: 'HIGH',
    },
};

export const mediumPriority: Story = {
    args: {
        priority: 'MEDIUM',
    },
};

export const lowPriority: Story = {
    args: {
        priority: 'LOW',
    },
};

export const unknownPriority: Story = {
    args: {
        priority: 'UNKNOWN',
    },
};
