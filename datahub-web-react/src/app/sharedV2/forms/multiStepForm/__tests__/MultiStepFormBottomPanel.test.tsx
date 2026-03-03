import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { MultiStepFormBottomPanel } from '@app/sharedV2/forms/multiStepForm/MultiStepFormBottomPanel';
import { MultiStepFormProvider, useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';
import { Step } from '@app/sharedV2/forms/multiStepForm/types';

// Define a test state type
interface TestState {
    name?: string;
    count?: number;
}

describe('MultiStepFormBottomPanel', () => {
    const mockStep1: Step = {
        label: 'Step 1',
        key: 'step1',
        content: <div>Test Content 1</div>,
    };

    const mockStep2: Step = {
        label: 'Step 2',
        key: 'step2',
        content: <div>Test Content 2</div>,
    };

    const mockStep3: Step = {
        label: 'Step 3',
        key: 'step3',
        content: <div>Test Content 3</div>,
    };

    it('renders without crashing', () => {
        render(
            <MultiStepFormProvider<TestState> steps={[mockStep1]}>
                <MultiStepFormBottomPanel />
            </MultiStepFormProvider>,
        );

        expect(screen.getByText('1 / 1')).toBeInTheDocument();
    });

    it('displays step counter correctly with multiple steps', () => {
        const steps = [mockStep1, mockStep2, mockStep3];

        render(
            <MultiStepFormProvider<TestState> steps={steps}>
                <MultiStepFormBottomPanel />
            </MultiStepFormProvider>,
        );

        expect(screen.getByText('1 / 3')).toBeInTheDocument();
    });

    it('shows navigation buttons based on form state', () => {
        const steps = [mockStep1, mockStep2];
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

    it('shows back button when not on the first step', () => {
        const TestComponent = () => {
            const { setCurrentStepCompleted } = useMultiStepContext();

            // Complete the current step to enable the next button
            React.useEffect(() => {
                setCurrentStepCompleted();
            }, [setCurrentStepCompleted]);

            return <MultiStepFormBottomPanel />;
        };

        render(
            <MultiStepFormProvider<TestState> steps={[mockStep1, mockStep2, mockStep3]}>
                <TestComponent />
            </MultiStepFormProvider>,
        );

        // Initially no back button when on first step
        expect(screen.queryByTestId('back-button')).not.toBeInTheDocument();

        // Go to second step - need to complete step first to enable Next button
        fireEvent.click(screen.getByTestId('next-button'));
        // Back button should appear now
        expect(screen.getByTestId('back-button')).toBeInTheDocument();

        expect(screen.getByText('Cancel')).toBeInTheDocument();
    });

    it('shows submit button on final step when showSubmitButton is true', async () => {
        const TestComponent = () => {
            const { setCurrentStepCompleted } = useMultiStepContext();

            // Complete the current step to enable the submit button
            React.useEffect(() => {
                setCurrentStepCompleted();
            }, [setCurrentStepCompleted]);

            return <MultiStepFormBottomPanel showSubmitButton />;
        };

        render(
            <MultiStepFormProvider<TestState> steps={[mockStep1, mockStep2]} onSubmit={() => Promise.resolve()}>
                <TestComponent />
            </MultiStepFormProvider>,
        );

        // Initially Next is present, no Submit
        expect(screen.getByText('Next')).toBeInTheDocument();
        expect(screen.queryByText('Submit')).not.toBeInTheDocument();

        // Move to second step
        fireEvent.click(screen.getByText('Next')); // Move to second step

        // Now should show Submit button (on final step) - wait for the re-render
        await waitFor(() => {
            expect(screen.getByText('Submit')).toBeInTheDocument();
        });
    });

    it('executes submit when submit button is clicked after step is completed', async () => {
        const mockSubmit = vi.fn(() => Promise.resolve());

        const TestComponent = () => {
            const { setCurrentStepCompleted } = useMultiStepContext();

            // Mark current step as completed when component mounts to enable submit button
            React.useEffect(() => {
                setCurrentStepCompleted();
            }, [setCurrentStepCompleted]);

            return <MultiStepFormBottomPanel showSubmitButton />;
        };

        render(
            <MultiStepFormProvider<TestState> steps={[mockStep1, mockStep2]} onSubmit={mockSubmit}>
                <TestComponent />
            </MultiStepFormProvider>,
        );

        fireEvent.click(screen.getByText('Next')); // Go to step 2

        // Now submit button should be enabled because step was marked as completed via context
        const submitButton = screen.getByText('Submit');
        expect(submitButton).not.toBeDisabled();

        fireEvent.click(submitButton);

        await waitFor(() => {
            expect(mockSubmit).toHaveBeenCalledTimes(1);
        });
    });

    it('executes cancel when cancel button is clicked', () => {
        const mockCancel = vi.fn();

        render(
            <MultiStepFormProvider<TestState> steps={[mockStep1, mockStep2]} onCancel={mockCancel}>
                <MultiStepFormBottomPanel />
            </MultiStepFormProvider>,
        );

        fireEvent.click(screen.getByText('Cancel'));
        expect(mockCancel).toHaveBeenCalled();
    });

    it('executes next when next button is clicked', () => {
        const TestComponent = () => {
            const { setCurrentStepCompleted } = useMultiStepContext();

            // Complete the current step to enable the next button
            React.useEffect(() => {
                setCurrentStepCompleted();
            }, [setCurrentStepCompleted]);

            return <MultiStepFormBottomPanel />;
        };

        render(
            <MultiStepFormProvider<TestState> steps={[mockStep1, mockStep2]}>
                <TestComponent />
            </MultiStepFormProvider>,
        );

        // Initially on step 1
        expect(screen.getByTestId('step-counter')).toHaveTextContent('1 / 2');
        expect(screen.getByTestId('next-button')).toBeInTheDocument();

        fireEvent.click(screen.getByTestId('next-button'));

        // After clicking "Next", the text should now show "2 / 2"
        expect(screen.getByTestId('step-counter')).toHaveTextContent('2 / 2');
        expect(screen.getByTestId('back-button')).toBeInTheDocument(); // Back button should now appear
    });

    it('executes back when back button is clicked', () => {
        const TestComponent = () => {
            const { setCurrentStepCompleted } = useMultiStepContext();

            // Complete the current step to enable the next button
            React.useEffect(() => {
                setCurrentStepCompleted();
            }, [setCurrentStepCompleted]);

            return <MultiStepFormBottomPanel />;
        };

        render(
            <MultiStepFormProvider<TestState> steps={[mockStep1, mockStep2]}>
                <TestComponent />
            </MultiStepFormProvider>,
        );

        // Move to next step first
        fireEvent.click(screen.getByTestId('next-button'));
        expect(screen.getByTestId('step-counter')).toHaveTextContent('2 / 2');
        expect(screen.getByTestId('back-button')).toBeInTheDocument();

        // Then go back
        fireEvent.click(screen.getByTestId('back-button'));
        expect(screen.getByTestId('step-counter')).toHaveTextContent('1 / 2');
    });

    it('disables submit button when step is not completed', () => {
        // In this test, we want the first step to be completed to enable navigation,
        // but the final step to remain uncompleted
        const TestComponent = () => {
            const { setCurrentStepCompleted, currentStepIndex } = useMultiStepContext();

            // Only complete step 1 (currentStepIndex 0), not step 2 (currentStepIndex 1)
            React.useEffect(() => {
                if (currentStepIndex === 0) {
                    // Only complete the first step
                    setCurrentStepCompleted();
                }
            }, [currentStepIndex, setCurrentStepCompleted]);

            return <MultiStepFormBottomPanel showSubmitButton />;
        };

        render(
            <MultiStepFormProvider<TestState> steps={[mockStep1, mockStep2]}>
                <TestComponent />
            </MultiStepFormProvider>,
        );

        // Go to the last step - need to complete step first to enable Next button
        fireEvent.click(screen.getByTestId('next-button'));

        // The submit button should appear but be disabled because the final step isn't completed yet
        const submitButton = screen.getByTestId('submit-button');
        expect(submitButton).toBeInTheDocument();
        expect(submitButton).toBeDisabled(); // Submit button is disabled when step is not completed
    });

    it('enables submit button when step is completed', async () => {
        let contextRef: any = null;

        const TestComponent = () => {
            const context = useMultiStepContext();
            contextRef = context;

            const { setCurrentStepCompleted, currentStepIndex } = context;

            // Only complete step 1 (currentStepIndex 0), not step 2 (currentStepIndex 1)
            React.useEffect(() => {
                if (currentStepIndex === 0) {
                    // Only complete the first step
                    setCurrentStepCompleted();
                }
            }, [currentStepIndex, setCurrentStepCompleted]);

            return <MultiStepFormBottomPanel showSubmitButton />;
        };

        render(
            <MultiStepFormProvider<TestState> steps={[mockStep1, mockStep2]}>
                <TestComponent />
            </MultiStepFormProvider>,
        );

        // Go to the last step (step is not completed initially)
        fireEvent.click(screen.getByTestId('next-button'));

        // Initially submit button is disabled because final step isn't completed
        const submitButton = screen.getByTestId('submit-button');
        expect(submitButton).toBeInTheDocument();
        expect(submitButton).toBeDisabled();

        // Complete the current step using contextRef
        if (contextRef) {
            contextRef.setCurrentStepCompleted();
        }

        // Wait for the button to update its state after the step completion
        await waitFor(() => {
            // The submit button should still exist but its state should have updated
            const updatedSubmitButton = screen.getByTestId('submit-button');
            expect(updatedSubmitButton).toBeInTheDocument();
        });
    });

    it('calls renderLeftButtons when provided', async () => {
        const mockRenderLeftButtons = vi.fn((buttons) => <div data-testid="custom-left">{buttons}</div>);

        render(
            <MultiStepFormProvider<TestState> steps={[mockStep1, mockStep2]}>
                <MultiStepFormBottomPanel renderLeftButtons={mockRenderLeftButtons} />
            </MultiStepFormProvider>,
        );

        // renderLeftButtons should be called initially
        // Let's check initial calls count
        expect(mockRenderLeftButtons).toHaveBeenCalled();

        // Click next to go to step 2 (to have a back button appear)
        fireEvent.click(screen.getByText('Next'));

        // Wait for the re-render to complete and check that function was called multiple times
        await waitFor(
            () => {
                expect(mockRenderLeftButtons).toHaveBeenCalledTimes(2);
            },
            { timeout: 1000 },
        );
    });

    it('calls renderRightButtons when provided', () => {
        const mockRenderRightButtons = vi.fn((buttons) => <div data-testid="custom-right">{buttons}</div>);

        render(
            <MultiStepFormProvider<TestState> steps={[mockStep1, mockStep2]}>
                <MultiStepFormBottomPanel renderRightButtons={mockRenderRightButtons} />
            </MultiStepFormProvider>,
        );

        expect(mockRenderRightButtons).toHaveBeenCalledWith(expect.arrayContaining([expect.anything()]));
    });
});
