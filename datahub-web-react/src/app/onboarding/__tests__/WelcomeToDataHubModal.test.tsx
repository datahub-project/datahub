import { MockedProvider } from '@apollo/client/testing';
import { act, fireEvent, render, screen } from '@testing-library/react';
import React from 'react';

import { useOnboardingTour } from '@app/onboarding/OnboardingTourContext.hooks';
import OnboardingTourProvider from '@app/onboarding/OnboardingTourContextProvider';
import { WelcomeToDataHubModal } from '@app/onboarding/WelcomeToDataHubModal';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

vi.mock('@images/FTE-search.mp4', () => ({ default: '/search.mp4' }));
vi.mock('@images/FTE-lineage.mp4', () => ({ default: '/lineage.mp4' }));
vi.mock('@images/FTE-impact.mp4', () => ({ default: '/impact.mp4' }));
vi.mock('@images/FTE-ai-documentation.mp4', () => ({ default: '/ai.mp4' }));

function ReopenTour() {
    const { triggerModalTour } = useOnboardingTour();
    return (
        <button type="button" onClick={triggerModalTour}>
            reopen tour
        </button>
    );
}

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
        // jsdom logs "Not implemented" for media playback. The pause behavior is
        // covered in the VideoSlide tests.
        vi.spyOn(HTMLMediaElement.prototype, 'play').mockImplementation(() => Promise.resolve());
        vi.spyOn(HTMLMediaElement.prototype, 'pause').mockImplementation(() => {});
    });

    afterEach(() => {
        vi.useRealTimers();
        vi.restoreAllMocks();
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

    it('does not show the tour or mount a video once it has been dismissed', () => {
        localStorage.setItem('skipWelcomeModal', 'true');
        renderTour();

        expect(screen.queryByText('Find Any Asset, Anywhere')).toBeNull();
        expect(document.querySelector('video')).toBeNull();
        expect(document.querySelector('img')).toBeNull();

        act(() => {
            vi.advanceTimersByTime(SLIDE_FALLBACK_MS);
        });

        expect(screen.queryByText('Find Any Asset, Anywhere')).toBeNull();
        expect(document.querySelector('video')).toBeNull();
        expect(carouselNext).not.toHaveBeenCalled();
    });

    it('unmounts the modal and videos on close and stays closed next visit', () => {
        const view = renderTour();
        expect(screen.getByText('Find Any Asset, Anywhere')).toBeInTheDocument();

        fireEvent.click(screen.getByTestId('modal-close-icon'));

        expect(localStorage.getItem('skipWelcomeModal')).toBe('true');
        expect(screen.queryByText('Find Any Asset, Anywhere')).toBeNull();
        expect(document.querySelector('video')).toBeNull();

        view.unmount();
        renderTour();

        expect(screen.queryByText('Find Any Asset, Anywhere')).toBeNull();
        expect(document.querySelector('video')).toBeNull();
        expect(document.querySelector('img')).toBeNull();
    });

    it('does not keep a prefetched clip playing on the next opening', async () => {
        render(
            <MockedProvider>
                <TestPageContainer>
                    <OnboardingTourProvider>
                        <WelcomeToDataHubModal />
                        <ReopenTour />
                    </OnboardingTourProvider>
                </TestPageContainer>
            </MockedProvider>,
        );

        await act(async () => {
            await Promise.resolve();
        });
        document.querySelectorAll('img').forEach((img) => fireEvent.load(img));

        const search = document.querySelector('video[src="/search.mp4"]') as HTMLVideoElement;
        expect(search).not.toBeNull();
        Object.defineProperty(search, 'duration', { configurable: true, value: 10 });
        Object.defineProperty(search, 'currentTime', { configurable: true, value: 9 });
        fireEvent.timeUpdate(search);
        await act(async () => {
            await Promise.resolve();
        });
        document.querySelectorAll('img').forEach((img) => fireEvent.load(img));
        expect(document.querySelector('video[src="/lineage.mp4"]')).not.toBeNull();

        fireEvent.click(screen.getByTestId('modal-close-icon'));
        fireEvent.click(screen.getByRole('button', { name: 'reopen tour' }));

        await act(async () => {
            await Promise.resolve();
        });
        document.querySelectorAll('img').forEach((img) => fireEvent.load(img));

        expect(document.querySelector('video[src="/search.mp4"]')).not.toBeNull();
        expect(document.querySelector('video[src="/lineage.mp4"]')).toBeNull();
    });

    it('keeps the prefetched video mounted when the next slide lands', async () => {
        renderTour();

        await act(async () => {
            await Promise.resolve();
        });
        document.querySelectorAll('img').forEach((img) => fireEvent.load(img));

        const search = document.querySelector('video[src="/search.mp4"]') as HTMLVideoElement;
        expect(search).not.toBeNull();
        Object.defineProperty(search, 'duration', { configurable: true, value: 10 });
        Object.defineProperty(search, 'currentTime', { configurable: true, value: 9 });
        fireEvent.timeUpdate(search);
        await act(async () => {
            await Promise.resolve();
        });
        document.querySelectorAll('img').forEach((img) => fireEvent.load(img));

        const lineage = document.querySelector('video[src="/lineage.mp4"]');
        expect(lineage).not.toBeNull();

        // slick fires afterChange from its own timer, outside a React event.
        slideChange.current?.(1);

        expect(document.querySelector('video[src="/lineage.mp4"]')).toBe(lineage);
        expect(document.querySelector('video[src="/search.mp4"]')).toBeNull();
    });
});
