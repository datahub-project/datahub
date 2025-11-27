import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { MultiStepFormBottomPanel } from '@app/sharedV2/forms/multiStepForm/MultiStepFormBottomPanel';
import { MultiStepFormProvider } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';
import { Step } from '@app/sharedV2/forms/multiStepForm/types';

// Define a test state type
interface TestState {
    name?: string;
    count?: number;
}

describe('MultiStepFormBottomPanel', () => {
    const mockStep: Step = {
        label: 'Test Step',
        key: 'test-step',
        content: <div>Test Content</div>,
    };

    it('renders without crashing', () => {
        render(
            <MultiStepFormProvider<TestState> steps={[mockStep]}>
                <MultiStepFormBottomPanel />
            </MultiStepFormProvider>,
        );

        expect(screen.getByText('1 / 1')).toBeInTheDocument();
    });

    it('displays step counter correctly with multiple steps', () => {
        const steps = [mockStep, mockStep, mockStep];

        render(
            <MultiStepFormProvider<TestState> steps={steps}>
                <MultiStepFormBottomPanel />
            </MultiStepFormProvider>,
        );

        expect(screen.getByText('1 / 3')).toBeInTheDocument();
    });

    it('shows navigation buttons based on form state', () => {
        const steps = [mockStep, mockStep];
        const mockSubmit = vi.fn();
        const mockCancel = vi.fn();

        render(
            <MultiStepFormProvider<TestState> steps={steps} onSubmit={mockSubmit} onCancel={mockCancel}>
                <MultiStepFormBottomPanel />
            </MultiStepFormProvider>,
        );

        // Initially on step 1 of 2, no back button, has next button
        expect(screen.queryByText('Back')).not.toBeInTheDocument();
        expect(screen.getByText('Next')).toBeInTheDocument();
        expect(screen.getByText('Cancel')).toBeInTheDocument();
        expect(screen.getByText('1 / 2')).toBeInTheDocument();
    });
});
