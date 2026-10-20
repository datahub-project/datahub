import { MockedProvider } from '@apollo/client/testing';
import { act, fireEvent, render } from '@testing-library/react';
import React from 'react';

import { WelcomeToDataHubModal } from '@app/onboarding/WelcomeToDataHubModal';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const { carouselNext, carouselPrev, slideChange } = vi.hoisted(() => ({
    carouselNext: vi.fn(),
    carouselPrev: vi.fn(),
    slideChange: { current: undefined as ((index: number) => void) | undefined },
}));

vi.mock('@src/alchemy-components/components/Carousel', async () => {
    const ReactActual = await import('react');
    return {
        Carousel: ReactActual.forwardRef(
            (
                props: { afterChange?: (index: number) => void; children?: React.ReactNode },
                ref: React.Ref<{ next: () => void; prev: () => void }>,
            ) => {
                slideChange.current = props.afterChange;
                ReactActual.useImperativeHandle(ref, () => ({
                    next: () => {
                        carouselNext();
                        props.afterChange?.(1);
                    },
                    prev: () => {
                        carouselPrev();
                    },
                }));
                return <div>{props.children}</div>;
            },
        ),
    };
});

const SLIDE_FALLBACK_MS = 25000;

function renderTour() {
    return render(
        <MockedProvider>
            <TestPageContainer>
                <WelcomeToDataHubModal />
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('WelcomeToDataHubModal slide advance', () => {
    beforeEach(() => {
        localStorage.clear();
        carouselNext.mockClear();
        carouselPrev.mockClear();
        slideChange.current = undefined;
        vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    it('still advances the first clip after ArrowLeft does not move', () => {
        renderTour();

        fireEvent.keyDown(document, { key: 'ArrowLeft' });

        expect(carouselPrev).not.toHaveBeenCalled();
        expect(carouselNext).not.toHaveBeenCalled();

        act(() => {
            vi.advanceTimersByTime(SLIDE_FALLBACK_MS);
        });

        expect(carouselNext).toHaveBeenCalledTimes(1);
    });

    it('advances again after a dot click returns to the slide we already left', () => {
        renderTour();

        act(() => {
            vi.advanceTimersByTime(SLIDE_FALLBACK_MS);
        });
        expect(carouselNext).toHaveBeenCalledTimes(1);

        act(() => {
            slideChange.current?.(0);
        });

        act(() => {
            vi.advanceTimersByTime(SLIDE_FALLBACK_MS);
        });

        expect(carouselNext).toHaveBeenCalledTimes(2);
    });

    it('does not step past the last slide', () => {
        renderTour();

        act(() => {
            slideChange.current?.(3);
        });
        fireEvent.keyDown(document, { key: 'ArrowRight' });

        expect(carouselNext).not.toHaveBeenCalled();
        expect(carouselPrev).not.toHaveBeenCalled();
    });
});
