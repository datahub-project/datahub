import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';
import styled from 'styled-components';

import { Stepper } from '@components/components/Stepper/Stepper';

const STEPS = [
    { title: 'Select' },
    { title: 'Define' },
    { title: 'Configure' },
    { title: 'Validate' },
    { title: 'Complete' },
];

const StepperWrapper = styled.div`
    width: 100%;
    max-width: 800px;
    padding: 40px;
`;

const meta = {
    title: 'Components/Stepper',
    component: Stepper,
    parameters: {
        layout: 'padded',
    },
    argTypes: {
        steps: {
            description: 'Array of step objects with titles',
            control: { type: 'object' },
        },
        currentStepIndex: {
            description: 'Index of the currently active step (0-based)',
            control: { type: 'number', min: 0, max: 4 },
        },
        dataTestId: {
            description: 'Optional test ID for testing',
            control: { type: 'text' },
        },
    },
    args: {
        steps: STEPS,
        currentStepIndex: 0,
    },
    tags: ['autodocs'],
} satisfies Meta<typeof Stepper>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
    args: {} as any,
    render: (args: any) => (
        <StepperWrapper>
            <Stepper {...args} />
        </StepperWrapper>
    ),
};

export const Step1Active: Story = {
    args: {
        currentStepIndex: 0,
    } as any,
    render: (args: any) => (
        <StepperWrapper>
            <Stepper {...args} />
        </StepperWrapper>
    ),
};

export const Step2Active: Story = {
    args: {
        currentStepIndex: 1,
    } as any,
    render: (args: any) => (
        <StepperWrapper>
            <Stepper {...args} />
        </StepperWrapper>
    ),
};

export const Step3Active: Story = {
    args: {
        currentStepIndex: 2,
    } as any,
    render: (args: any) => (
        <StepperWrapper>
            <Stepper {...args} />
        </StepperWrapper>
    ),
};

export const LastStepCompleted: Story = {
    args: {
        currentStepIndex: 4,
    } as any,
    render: (args: any) => (
        <StepperWrapper>
            <Stepper {...args} />
        </StepperWrapper>
    ),
};

export const ThreeSteps: Story = {
    args: {
        steps: [{ title: 'Upload' }, { title: 'Review' }, { title: 'Publish' }],
        currentStepIndex: 1,
    } as any,
    render: (args: any) => (
        <StepperWrapper>
            <Stepper {...args} />
        </StepperWrapper>
    ),
};

export const LongStepTitles: Story = {
    args: {
        steps: [
            { title: 'Select Properties' },
            { title: 'Define Conditions' },
            { title: 'Configure Actions' },
            { title: 'Validate & Review' },
        ],
        currentStepIndex: 2,
    } as any,
    render: (args: any) => (
        <StepperWrapper>
            <Stepper {...args} />
        </StepperWrapper>
    ),
};
