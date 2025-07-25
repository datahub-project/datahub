import { fireEvent, render, screen } from '@testing-library/react';
import React, { useContext } from 'react';

import { OnboardingTourContext } from '@app/onboarding/OnboardingTourContext';
import OnboardingTourContextProvider from '@app/onboarding/OnboardingTourContextProvider';

// Test component to consume the context
const TestConsumer: React.FC = () => {
    const context = useContext(OnboardingTourContext);

    if (!context) {
        return <div data-testid="no-context">No context provided</div>;
    }

    const {
        isModalTourOpen,
        triggerModalTour,
        closeModalTour,
        triggerOriginalTour,
        closeOriginalTour,
        originalTourStepIds,
    } = context;

    return (
        <div>
            <div data-testid="modal-tour-open">{isModalTourOpen.toString()}</div>
            <div data-testid="original-tour-steps">{originalTourStepIds ? originalTourStepIds.join(',') : 'null'}</div>
            <button type="button" data-testid="trigger-modal-tour" onClick={triggerModalTour}>
                Trigger Modal Tour
            </button>
            <button type="button" data-testid="close-modal-tour" onClick={closeModalTour}>
                Close Modal Tour
            </button>
            <button
                type="button"
                data-testid="trigger-original-tour"
                onClick={() => triggerOriginalTour(['step1', 'step2', 'step3'])}
            >
                Trigger Original Tour
            </button>
            <button type="button" data-testid="close-original-tour" onClick={closeOriginalTour}>
                Close Original Tour
            </button>
        </div>
    );
};

describe('OnboardingTourContextProvider', () => {
    it('should provide initial state values', () => {
        render(
            <OnboardingTourContextProvider>
                <TestConsumer />
            </OnboardingTourContextProvider>,
        );

        expect(screen.getByTestId('modal-tour-open')).toHaveTextContent('false');
        expect(screen.getByTestId('original-tour-steps')).toHaveTextContent('null');
    });

    it('should render children correctly', () => {
        render(
            <OnboardingTourContextProvider>
                <div data-testid="child-component">Test child</div>
            </OnboardingTourContextProvider>,
        );

        expect(screen.getByTestId('child-component')).toBeInTheDocument();
        expect(screen.getByText('Test child')).toBeInTheDocument();
    });

    describe('Modal Tour functionality', () => {
        it('should trigger modal tour', () => {
            render(
                <OnboardingTourContextProvider>
                    <TestConsumer />
                </OnboardingTourContextProvider>,
            );

            // Initial state
            expect(screen.getByTestId('modal-tour-open')).toHaveTextContent('false');

            // Trigger modal tour
            fireEvent.click(screen.getByTestId('trigger-modal-tour'));

            expect(screen.getByTestId('modal-tour-open')).toHaveTextContent('true');
        });

        it('should close modal tour', () => {
            render(
                <OnboardingTourContextProvider>
                    <TestConsumer />
                </OnboardingTourContextProvider>,
            );

            // First trigger the modal tour
            fireEvent.click(screen.getByTestId('trigger-modal-tour'));
            expect(screen.getByTestId('modal-tour-open')).toHaveTextContent('true');

            // Then close it
            fireEvent.click(screen.getByTestId('close-modal-tour'));
            expect(screen.getByTestId('modal-tour-open')).toHaveTextContent('false');
        });

        it('should handle multiple trigger/close cycles', () => {
            render(
                <OnboardingTourContextProvider>
                    <TestConsumer />
                </OnboardingTourContextProvider>,
            );

            // Multiple cycles
            fireEvent.click(screen.getByTestId('trigger-modal-tour'));
            expect(screen.getByTestId('modal-tour-open')).toHaveTextContent('true');

            fireEvent.click(screen.getByTestId('close-modal-tour'));
            expect(screen.getByTestId('modal-tour-open')).toHaveTextContent('false');

            fireEvent.click(screen.getByTestId('trigger-modal-tour'));
            expect(screen.getByTestId('modal-tour-open')).toHaveTextContent('true');
        });
    });

    describe('Original Tour functionality', () => {
        it('should trigger original tour with step IDs', () => {
            render(
                <OnboardingTourContextProvider>
                    <TestConsumer />
                </OnboardingTourContextProvider>,
            );

            // Initial state
            expect(screen.getByTestId('original-tour-steps')).toHaveTextContent('null');

            // Trigger original tour
            fireEvent.click(screen.getByTestId('trigger-original-tour'));

            expect(screen.getByTestId('original-tour-steps')).toHaveTextContent('step1,step2,step3');
        });

        it('should close original tour', () => {
            render(
                <OnboardingTourContextProvider>
                    <TestConsumer />
                </OnboardingTourContextProvider>,
            );

            // First trigger the original tour
            fireEvent.click(screen.getByTestId('trigger-original-tour'));
            expect(screen.getByTestId('original-tour-steps')).toHaveTextContent('step1,step2,step3');

            // Then close it
            fireEvent.click(screen.getByTestId('close-original-tour'));
            expect(screen.getByTestId('original-tour-steps')).toHaveTextContent('null');
        });

        it('should handle multiple trigger/close cycles for original tour', () => {
            render(
                <OnboardingTourContextProvider>
                    <TestConsumer />
                </OnboardingTourContextProvider>,
            );

            // Multiple cycles
            fireEvent.click(screen.getByTestId('trigger-original-tour'));
            expect(screen.getByTestId('original-tour-steps')).toHaveTextContent('step1,step2,step3');

            fireEvent.click(screen.getByTestId('close-original-tour'));
            expect(screen.getByTestId('original-tour-steps')).toHaveTextContent('null');

            fireEvent.click(screen.getByTestId('trigger-original-tour'));
            expect(screen.getByTestId('original-tour-steps')).toHaveTextContent('step1,step2,step3');
        });
    });

    describe('Context integration', () => {
        it('should handle both modal and original tours simultaneously', () => {
            render(
                <OnboardingTourContextProvider>
                    <TestConsumer />
                </OnboardingTourContextProvider>,
            );

            // Trigger both
            fireEvent.click(screen.getByTestId('trigger-modal-tour'));
            fireEvent.click(screen.getByTestId('trigger-original-tour'));

            expect(screen.getByTestId('modal-tour-open')).toHaveTextContent('true');
            expect(screen.getByTestId('original-tour-steps')).toHaveTextContent('step1,step2,step3');

            // Close both
            fireEvent.click(screen.getByTestId('close-modal-tour'));
            fireEvent.click(screen.getByTestId('close-original-tour'));

            expect(screen.getByTestId('modal-tour-open')).toHaveTextContent('false');
            expect(screen.getByTestId('original-tour-steps')).toHaveTextContent('null');
        });

        it('should provide context to multiple children', () => {
            render(
                <OnboardingTourContextProvider>
                    <TestConsumer />
                    <div data-testid="sibling">Sibling component</div>
                </OnboardingTourContextProvider>,
            );

            expect(screen.getByTestId('modal-tour-open')).toBeInTheDocument();
            expect(screen.getByTestId('sibling')).toBeInTheDocument();
        });
    });

    describe('Edge cases', () => {
        it('should handle empty step IDs array', () => {
            const EmptyStepsConsumer: React.FC = () => {
                const context = useContext(OnboardingTourContext);
                return (
                    <button
                        type="button"
                        data-testid="trigger-empty-tour"
                        onClick={() => context?.triggerOriginalTour([])}
                    >
                        Trigger Empty Tour
                    </button>
                );
            };

            render(
                <OnboardingTourContextProvider>
                    <TestConsumer />
                    <EmptyStepsConsumer />
                </OnboardingTourContextProvider>,
            );

            fireEvent.click(screen.getByTestId('trigger-empty-tour'));
            expect(screen.getByTestId('original-tour-steps')).toHaveTextContent('');
        });

        it('should handle single step ID', () => {
            const SingleStepConsumer: React.FC = () => {
                const context = useContext(OnboardingTourContext);
                return (
                    <button
                        type="button"
                        data-testid="trigger-single-tour"
                        onClick={() => context?.triggerOriginalTour(['single-step'])}
                    >
                        Trigger Single Tour
                    </button>
                );
            };

            render(
                <OnboardingTourContextProvider>
                    <TestConsumer />
                    <SingleStepConsumer />
                </OnboardingTourContextProvider>,
            );

            fireEvent.click(screen.getByTestId('trigger-single-tour'));
            expect(screen.getByTestId('original-tour-steps')).toHaveTextContent('single-step');
        });
    });
});
