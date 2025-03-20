import React from 'react';
import { Meta, StoryObj } from '@storybook/react';
import { IncidentStagePill } from './IncidentStagePill';

const meta: Meta<typeof IncidentStagePill> = {
    title: 'Components / IncidentStagePill',
    component: IncidentStagePill,

    // Component-level parameters
    parameters: {
        layout: 'centered',
        docs: {
            subtitle: 'Displays a pill representing the current stage of an incident.',
        },
    },

    // Component-level argTypes
    argTypes: {
        stage: {
            description: 'The current stage of the incident.',
            control: 'select',
            options: ['TRIAGE', 'INVESTIGATION', 'WORK_IN_PROGRESS', 'FIXED', 'NO_ACTION_REQUIRED'],
            table: {
                type: { summary: 'string' },
                defaultValue: { summary: 'TRIAGE' },
            },
        },
        showLabel: {
            description: 'Controls whether the label should be displayed.',
            table: {
                defaultValue: { summary: 'true' }, // Assuming true is the default
            },
            control: {
                type: 'boolean',
            },
        },
    },

    // Default props
    args: {
        stage: 'WORK_IN_PROGRESS',
        showLabel: true,
    },
};

export default meta;

type Story = StoryObj<typeof meta>;

// Sandbox Story
export const sandbox: Story = {
    render: (props) => <IncidentStagePill {...props} />,
};

// Example Stories
export const triageStage: Story = {
    args: {
        stage: 'FIXED',
    },
};

export const investigationStage: Story = {
    args: {
        stage: 'INVESTIGATION',
    },
};

export const inProgressStage: Story = {
    args: {
        stage: 'WORK_IN_PROGRESS',
    },
};

export const resolvedStage: Story = {
    args: {
        stage: 'FIXED',
    },
};

export const noActionStage: Story = {
    args: {
        stage: 'NO_ACTION_REQUIRED',
    },
};

export const unknownStage: Story = {
    args: {
        stage: 'UNKNOWN',
    },
};
