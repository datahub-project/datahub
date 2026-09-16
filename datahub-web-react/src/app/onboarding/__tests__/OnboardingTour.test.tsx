import { render } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';

import OnboardingContext from '@app/onboarding/OnboardingContext';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import themeV2 from '@conf/theme/themeV2';
import { EducationStepsContext } from '@providers/EducationStepsContext';

// Mock dependencies
vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => ({ user: { urn: 'urn:li:corpuser:test' } }),
}));
vi.mock('@app/onboarding/useShouldSkipOnboardingTour', () => ({
    default: () => false,
}));
vi.mock('@graphql/step.generated', () => ({
    useBatchUpdateStepStatesMutation: () => [vi.fn(), {}],
}));
vi.mock('@app/onboarding/utils', () => ({
    getStepsToRender: () => [{ id: 'test-step', content: 'Test' }],
    convertStepId: vi.fn(),
    getConditionalStepIdsToAdd: () => [],
}));
vi.mock('reactour', () => ({
    default: ({ isOpen }: any) => (isOpen ? <div data-testid="tour" /> : null),
}));

describe('OnboardingTour', () => {
    const mockSetIsTourOpen = vi.fn();

    const renderTour = (pathname: string, isTourOpen = false) => {
        return render(
            <ThemeProvider theme={themeV2}>
                <MemoryRouter initialEntries={[pathname]}>
                    <EducationStepsContext.Provider
                        value={{
                            educationSteps: [],
                            setEducationSteps: vi.fn(),
                            educationStepIdsAllowlist: new Set(['test-step']),
                            setEducationStepIdsAllowlist: vi.fn(),
                        }}
                    >
                        <OnboardingContext.Provider
                            value={{
                                isTourOpen,
                                tourReshow: false,
                                setTourReshow: vi.fn(),
                                setIsTourOpen: mockSetIsTourOpen,
                                isUserInitializing: false,
                                setIsUserInitializing: vi.fn(),
                                isOnboardingAvailable: false,
                                setIsOnboardingAvailable: vi.fn(),
                            }}
                        >
                            <OnboardingTour stepIds={['test-step']} />
                        </OnboardingContext.Provider>
                    </EducationStepsContext.Provider>
                </MemoryRouter>
            </ThemeProvider>,
        );
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should not show tour on homepage', () => {
        const { queryByTestId } = renderTour('/', true);
        expect(queryByTestId('tour')).not.toBeInTheDocument();
    });

    it('should show tour on other pages when open', () => {
        const { getByTestId } = renderTour('/ingestion', true);
        expect(getByTestId('tour')).toBeInTheDocument();
    });

    it('should auto-open tour on non-homepage with steps', () => {
        renderTour('/ingestion');
        expect(mockSetIsTourOpen).toHaveBeenCalledWith(true);
    });

    it('should not auto-open tour on homepage', () => {
        renderTour('/');
        expect(mockSetIsTourOpen).not.toHaveBeenCalled();
    });
});
