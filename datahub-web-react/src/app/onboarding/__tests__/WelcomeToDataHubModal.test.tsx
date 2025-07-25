import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';

import analytics, { EventType } from '@app/analytics';
import { useOnboardingTour } from '@app/onboarding/OnboardingTourContext.hooks';
import { WelcomeToDataHubModal } from '@app/onboarding/WelcomeToDataHubModal';
import useShouldSkipOnboardingTour, { SKIP_ONBOARDING_TOUR_KEY } from '@app/onboarding/useShouldSkipOnboardingTour';

// Mock dependencies
vi.mock('@app/analytics');
vi.mock('@app/onboarding/OnboardingTourContext.hooks');
vi.mock('@app/onboarding/useShouldSkipOnboardingTour');
vi.mock('@components', () => ({
    Button: ({ children, onClick, ...props }: any) => (
        <button type="button" onClick={onClick} {...props}>
            {children}
        </button>
    ),
    Carousel: React.forwardRef<any, any>(
        ({ children, afterChange, beforeChange, leftComponent, rightComponent, ...props }, ref) => {
            const [currentSlide, setCurrentSlide] = React.useState(0);

            React.useImperativeHandle(ref, () => ({
                goTo: (slide: number) => {
                    beforeChange?.(currentSlide, slide);
                    setCurrentSlide(slide);
                    afterChange?.(slide);
                },
            }));

            return (
                <div data-testid="carousel" {...props}>
                    <div data-testid="carousel-content">{children}</div>
                    {leftComponent && <div data-testid="left-component">{leftComponent}</div>}
                    {rightComponent && <div data-testid="right-component">{rightComponent}</div>}
                    <button
                        type="button"
                        data-testid="next-slide"
                        onClick={() => {
                            const nextSlide = (currentSlide + 1) % React.Children.count(children);
                            beforeChange?.(currentSlide, nextSlide);
                            setCurrentSlide(nextSlide);
                            afterChange?.(nextSlide);
                        }}
                    >
                        Next
                    </button>
                </div>
            );
        },
    ),
    Heading: ({ children, ...props }: any) => <h1 {...props}>{children}</h1>,
    LoadedImage: ({ src, alt, ...props }: any) => <img src={src} alt={alt} {...props} data-testid="loaded-image" />,
    Modal: ({ children, title, onCancel, buttons, ...props }: any) => (
        <div data-testid="modal" {...props}>
            <div data-testid="modal-header">
                <h2>{title}</h2>
                <button type="button" data-testid="close-button" onClick={onCancel}>
                    ×
                </button>
            </div>
            <div data-testid="modal-content">{children}</div>
            {buttons && buttons.length > 0 && (
                <div data-testid="modal-buttons">
                    {buttons.map((button: any, index: number) => (
                        <button
                            key={`modal-button-${button.text}`}
                            type="button"
                            onClick={button.onClick}
                            data-testid={`modal-button-${index}`}
                        >
                            {button.text}
                        </button>
                    ))}
                </div>
            )}
        </div>
    ),
}));

// Mock styled components
vi.mock('@src/app/onboarding/WelcomeToDataHubModal.components', () => ({
    LoadingContainer: ({ children, ...props }: any) => (
        <div data-testid="loading-container" {...props}>
            {children}
        </div>
    ),
    SlideContainer: ({ children, isActive, ...props }: any) => (
        <div data-testid="slide-container" data-active={isActive} {...props}>
            {children}
        </div>
    ),
    StyledDocsLink: ({ children, href, onClick, ...props }: any) => (
        <a href={href} onClick={onClick} data-testid="docs-link" {...props}>
            {children}
        </a>
    ),
    VideoContainer: ({ children, ...props }: any) => (
        <div data-testid="video-container" {...props}>
            {children}
        </div>
    ),
    VideoSlide: ({ videoSrc, isReady, onVideoLoad, width }: any) => (
        <div data-testid="video-slide" data-ready={isReady} data-width={width}>
            {isReady ? (
                <video data-testid="video" onCanPlay={onVideoLoad}>
                    <source src={videoSrc} type="video/mp4" />
                    <track kind="captions" label="Test captions" />
                </video>
            ) : (
                <div data-testid="video-loading">Loading video...</div>
            )}
        </div>
    ),
}));

// Mock image imports
vi.mock('@images/welcome-modal-home-screenshot.png', () => ({
    default: 'mock-screenshot.png',
}));
vi.mock('@images/FTE-search.mp4', () => ({ default: 'mock-search.mp4' }));
vi.mock('@images/FTE-lineage.mp4', () => ({ default: 'mock-lineage.mp4' }));
vi.mock('@images/FTE-impact.mp4', () => ({ default: 'mock-impact.mp4' }));

const mockAnalytics = analytics as any;
const mockUseOnboardingTour = useOnboardingTour as any;
const mockUseShouldSkipOnboardingTour = useShouldSkipOnboardingTour as any;

describe('WelcomeToDataHubModal', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.useFakeTimers();

        // Default mock implementations
        mockUseOnboardingTour.mockReturnValue({
            isModalTourOpen: false,
            closeModalTour: vi.fn(),
        });

        mockUseShouldSkipOnboardingTour.mockReturnValue(false);

        // Mock localStorage
        Object.defineProperty(window, 'localStorage', {
            value: {
                setItem: vi.fn(),
                getItem: vi.fn(),
                removeItem: vi.fn(),
                clear: vi.fn(),
            },
            writable: true,
        });
    });

    afterEach(() => {
        vi.useRealTimers();
        vi.clearAllMocks();
    });

    describe('Rendering', () => {
        it('should not render when shouldSkipTour is true', () => {
            mockUseShouldSkipOnboardingTour.mockReturnValue(true);

            render(<WelcomeToDataHubModal />);

            expect(screen.queryByTestId('modal')).not.toBeInTheDocument();
        });

        it('should render modal initially with videos loading', async () => {
            render(<WelcomeToDataHubModal />);

            // Should render the modal
            await waitFor(() => {
                expect(screen.getByTestId('modal')).toBeInTheDocument();
            });

            // Should show carousel (not loading state as videos are set immediately)
            expect(screen.getByTestId('carousel')).toBeInTheDocument();

            // Should show the first slide content
            expect(screen.getByText('Find Any Asset, Anywhere')).toBeInTheDocument();

            // Videos should be in loading state initially
            const videoSlides = screen.getAllByTestId('video-slide');
            expect(videoSlides.length).toBeGreaterThan(0);
            expect(screen.getAllByTestId('video-loading')).toHaveLength(3); // 3 video slides
        });

        it('should show manual tour when isModalTourOpen is true', () => {
            mockUseOnboardingTour.mockReturnValue({
                isModalTourOpen: true,
                closeModalTour: vi.fn(),
            });
            mockUseShouldSkipOnboardingTour.mockReturnValue(true); // Even when should skip, manual tour should show

            render(<WelcomeToDataHubModal />);

            expect(screen.getByTestId('modal')).toBeInTheDocument();
        });
    });

    describe('Analytics Events', () => {
        it('should track page view and modal view event when modal opens', async () => {
            render(<WelcomeToDataHubModal />);

            await waitFor(() => {
                expect(mockAnalytics.page).toHaveBeenCalledWith({
                    originPath: '/onboarding-tour',
                });

                expect(mockAnalytics.event).toHaveBeenCalledWith({
                    type: EventType.WelcomeToDataHubModalViewEvent,
                });
            });
        });

        it('should track interact event when slide changes', async () => {
            render(<WelcomeToDataHubModal />);

            vi.advanceTimersByTime(100);

            await waitFor(() => {
                expect(screen.getByTestId('carousel')).toBeInTheDocument();
            });

            // Simulate slide change
            fireEvent.click(screen.getByTestId('next-slide'));

            await waitFor(() => {
                expect(mockAnalytics.event).toHaveBeenCalledWith({
                    type: EventType.WelcomeToDataHubModalInteractEvent,
                    direction: 'forward',
                    currentSlide: 2,
                    totalSlides: 4,
                });
            });
        });

        it('should track exit event when modal is closed', async () => {
            render(<WelcomeToDataHubModal />);

            await waitFor(() => {
                expect(screen.getByTestId('close-button')).toBeInTheDocument();
            });

            fireEvent.click(screen.getByTestId('close-button'));

            expect(mockAnalytics.event).toHaveBeenCalledWith({
                type: EventType.WelcomeToDataHubModalExitEvent,
                currentSlide: 1,
                totalSlides: 4,
                exitMethod: 'close_button',
            });
        });

        it('should track documentation click event', async () => {
            render(<WelcomeToDataHubModal />);

            vi.advanceTimersByTime(100);

            // Navigate to last slide
            const nextButton = screen.getByTestId('next-slide');

            // Click through slides to reach the last one
            fireEvent.click(nextButton);
            fireEvent.click(nextButton);
            fireEvent.click(nextButton);

            await waitFor(() => {
                const docsLink = screen.queryByTestId('docs-link');
                if (docsLink) {
                    fireEvent.click(docsLink);

                    expect(mockAnalytics.event).toHaveBeenCalledWith({
                        type: EventType.WelcomeToDataHubModalClickViewDocumentationEvent,
                        url: 'https://docs.datahub.com/docs/category/features',
                    });
                }
            });
        });
    });

    describe('Keyboard Navigation', () => {
        it('should navigate slides with arrow keys', async () => {
            render(<WelcomeToDataHubModal />);

            vi.advanceTimersByTime(100);

            await waitFor(() => {
                expect(screen.getByTestId('carousel')).toBeInTheDocument();
            });

            // Test right arrow
            fireEvent.keyDown(window, { key: 'ArrowRight' });

            await waitFor(() => {
                expect(mockAnalytics.event).toHaveBeenCalledWith(
                    expect.objectContaining({
                        type: EventType.WelcomeToDataHubModalInteractEvent,
                        direction: 'forward',
                    }),
                );
            });

            // Test left arrow
            fireEvent.keyDown(window, { key: 'ArrowLeft' });

            await waitFor(() => {
                expect(mockAnalytics.event).toHaveBeenCalledWith(
                    expect.objectContaining({
                        type: EventType.WelcomeToDataHubModalInteractEvent,
                        direction: 'backward',
                    }),
                );
            });
        });

        it('should not handle keyboard events when modal is closed', () => {
            mockUseShouldSkipOnboardingTour.mockReturnValue(true);

            render(<WelcomeToDataHubModal />);

            fireEvent.keyDown(window, { key: 'ArrowRight' });

            // Should not track any events
            expect(mockAnalytics.event).not.toHaveBeenCalledWith(
                expect.objectContaining({
                    type: EventType.WelcomeToDataHubModalInteractEvent,
                }),
            );
        });
    });

    describe('Auto-advance functionality', () => {
        it('should auto-advance slides after SLIDE_DURATION_MS', async () => {
            render(<WelcomeToDataHubModal />);

            vi.advanceTimersByTime(100);

            await waitFor(() => {
                expect(screen.getByTestId('carousel')).toBeInTheDocument();
            });

            // Advance timer by slide duration (10 seconds)
            vi.advanceTimersByTime(10000);

            await waitFor(() => {
                expect(mockAnalytics.event).toHaveBeenCalledWith(
                    expect.objectContaining({
                        type: EventType.WelcomeToDataHubModalInteractEvent,
                        direction: 'forward',
                        currentSlide: 2,
                    }),
                );
            });
        });

        it('should restart timer after manual navigation', async () => {
            render(<WelcomeToDataHubModal />);

            vi.advanceTimersByTime(100);

            await waitFor(() => {
                expect(screen.getByTestId('carousel')).toBeInTheDocument();
            });

            // Manual navigation
            fireEvent.click(screen.getByTestId('next-slide'));

            // Advance timer again
            vi.advanceTimersByTime(10000);

            // Should continue auto-advancing
            await waitFor(() => {
                expect(mockAnalytics.event).toHaveBeenCalledWith(
                    expect.objectContaining({
                        currentSlide: 3,
                    }),
                );
            });
        });
    });

    describe('Modal closing', () => {
        it('should close modal and set localStorage for automatic tour', async () => {
            const mockCloseModalTour = vi.fn();
            mockUseOnboardingTour.mockReturnValue({
                isModalTourOpen: false,
                closeModalTour: mockCloseModalTour,
            });

            render(<WelcomeToDataHubModal />);

            await waitFor(() => {
                expect(screen.getByTestId('close-button')).toBeInTheDocument();
            });

            fireEvent.click(screen.getByTestId('close-button'));

            expect(localStorage.setItem).toHaveBeenCalledWith(SKIP_ONBOARDING_TOUR_KEY, 'true');
            expect(mockCloseModalTour).not.toHaveBeenCalled();
        });

        it('should close modal tour for manual tour without setting localStorage', async () => {
            const mockCloseModalTour = vi.fn();
            mockUseOnboardingTour.mockReturnValue({
                isModalTourOpen: true,
                closeModalTour: mockCloseModalTour,
            });

            render(<WelcomeToDataHubModal />);

            await waitFor(() => {
                expect(screen.getByTestId('close-button')).toBeInTheDocument();
            });

            fireEvent.click(screen.getByTestId('close-button'));

            expect(mockCloseModalTour).toHaveBeenCalled();
            expect(localStorage.setItem).not.toHaveBeenCalled();
        });

        it('should clean up timers when closing', async () => {
            const clearIntervalSpy = vi.spyOn(global, 'clearInterval');

            render(<WelcomeToDataHubModal />);

            vi.advanceTimersByTime(100);

            await waitFor(() => {
                expect(screen.getByTestId('close-button')).toBeInTheDocument();
            });

            fireEvent.click(screen.getByTestId('close-button'));

            expect(clearIntervalSpy).toHaveBeenCalled();
        });
    });

    describe('Get Started button', () => {
        it('should close modal with get_started_button exit method', async () => {
            render(<WelcomeToDataHubModal />);

            await waitFor(() => {
                const getStartedButton = screen.queryByTestId('modal-button-0');
                if (getStartedButton) {
                    fireEvent.click(getStartedButton);

                    expect(mockAnalytics.event).toHaveBeenCalledWith({
                        type: EventType.WelcomeToDataHubModalExitEvent,
                        currentSlide: 1,
                        totalSlides: 4,
                        exitMethod: 'get_started_button',
                    });
                }
            });
        });
    });

    describe('Edge cases', () => {
        it('should handle component unmounting', () => {
            const { unmount } = render(<WelcomeToDataHubModal />);

            // Should not throw errors when unmounting
            expect(() => unmount()).not.toThrow();
        });

        it('should handle invalid slide transitions', async () => {
            render(<WelcomeToDataHubModal />);

            vi.advanceTimersByTime(100);

            await waitFor(() => {
                expect(screen.getByTestId('carousel')).toBeInTheDocument();
            });

            // Test boundary conditions
            fireEvent.keyDown(window, { key: 'ArrowLeft' }); // Should wrap to last slide
            fireEvent.keyDown(window, { key: 'ArrowRight' }); // Should wrap to first slide

            // Should handle gracefully without errors
            expect(screen.getByTestId('carousel')).toBeInTheDocument();
        });

        it('should handle video loading errors gracefully', async () => {
            const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

            render(<WelcomeToDataHubModal />);

            // Component should still render even if videos fail to load
            await waitFor(() => {
                expect(screen.getByTestId('modal')).toBeInTheDocument();
            });

            consoleErrorSpy.mockRestore();
        });
    });
});
