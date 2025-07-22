import { Button, Carousel, Heading, LoadedImage, Modal } from '@components';
import React, { useEffect, useRef, useState } from 'react';

import analytics, { EventType } from '@app/analytics';
import { useIsDocumentationInferenceEnabled } from '@app/entityV2/shared/components/inferredDocs/utils';
import { useOnboardingTour } from '@app/onboarding/OnboardingTourContext.hooks';
import useShouldSkipOnboardingTour, { SKIP_ONBOARDING_TOUR_KEY } from '@app/onboarding/useShouldSkipOnboardingTour';
import {
    LoadingContainer,
    SlideContainer,
    StyledDocsLink,
    VideoContainer,
} from '@src/app/onboarding/WelcomeToDataHubModal.components';

const SLIDE_DURATION_MS = 10000;
const DATAHUB_DOCS_URL = 'https://docs.datahub.com/docs/category/features';
const WELCOME_TO_DATAHUB_MODAL_TITLE = 'Welcome to DataHub';
const WELCOME_MODAL_HOME_SCREENSHOT_PATH = '/src/images/welcome-modal-home-screenshot.png';

interface VideoSources {
    search: string;
    lineage: string;
    impact: string;
    aiDocs?: string;
}

export const WelcomeToDataHubModal = () => {
    const [shouldShow, setShouldShow] = useState(false);
    const [currentSlide, setCurrentSlide] = useState(0);
    const [videoSources, setVideoSources] = useState<VideoSources | null>(null);
    const [videoLoading, setVideoLoading] = useState(false);
    const [videosReady, setVideosReady] = useState<{ [key in keyof VideoSources]?: boolean }>({});
    const hasTrackedView = useRef(false);
    const slideTimer = useRef<NodeJS.Timeout | null>(null);
    const carouselRef = useRef<any>(null);
    const { isModalTourOpen, closeModalTour } = useOnboardingTour();
    const shouldSkipTour = useShouldSkipOnboardingTour();
    const isDocumentationSlideEnabled = useIsDocumentationInferenceEnabled();
    const TOTAL_CAROUSEL_SLIDES = isDocumentationSlideEnabled ? 5 : 4;
    const MODAL_IMAGE_WIDTH_RAW = 620;
    const MODAL_IMAGE_WIDTH = `${MODAL_IMAGE_WIDTH_RAW}px`;
    const MODAL_WIDTH_NUM = MODAL_IMAGE_WIDTH_RAW + 45; // Add padding
    const MODAL_WIDTH = `${MODAL_WIDTH_NUM}px`;

    // Automatic tour for first-time home page visitors
    useEffect(() => {
        if (!shouldSkipTour) {
            setShouldShow(true);
            setCurrentSlide(0);
        }
    }, [shouldSkipTour]);

    // Manual tour trigger from Product Tour buttons
    useEffect(() => {
        if (isModalTourOpen) {
            setShouldShow(true);
            setCurrentSlide(0);
        }
    }, [isModalTourOpen]);

    // Show modal immediately, load videos individually as they complete
    useEffect(() => {
        if (shouldShow && !videoSources) {
            // Show modal immediately with empty video sources
            const emptyVideoSources: VideoSources = {
                search: '',
                lineage: '',
                impact: '',
                aiDocs: undefined,
            };
            setVideoSources(emptyVideoSources);
            setVideoLoading(false);

            // Load all videos in parallel, update each as it completes
            const loadVideo = async (videoKey: keyof VideoSources, importPromise: Promise<{ default: string }>) => {
                try {
                    const module = await importPromise;
                    setVideoSources((prev) => (prev ? { ...prev, [videoKey]: module.default } : prev));
                } catch (error) {
                    console.error(`Failed to load ${videoKey} video:`, error);
                }
            };

            // Start loading all videos simultaneously
            loadVideo('search', import('@images/FTE-search.mp4'));
            loadVideo('lineage', import('@images/FTE-lineage.mp4'));
            loadVideo('impact', import('@images/FTE-impact.mp4'));

            if (isDocumentationSlideEnabled) {
                loadVideo('aiDocs', import('@images/FTE-ai-documentation.mp4'));
            }
        }
    }, [isDocumentationSlideEnabled, shouldShow, videoSources]);

    // Handle when video elements are fully loaded
    const handleVideoLoad = (videoKey: keyof VideoSources) => {
        setVideosReady((prev) => ({ ...prev, [videoKey]: true }));
    };

    // Track page view when modal opens
    useEffect(() => {
        if (shouldShow && !hasTrackedView.current) {
            analytics.page({
                originPath: '/onboarding-tour',
            });

            analytics.event({
                type: EventType.WelcomeToDataHubModalViewEvent,
            });

            hasTrackedView.current = true;
        }
    }, [shouldShow]);

    // Auto-advance slides
    useEffect(() => {
        if (shouldShow && videoSources) {
            slideTimer.current = setInterval(() => {
                setCurrentSlide((prev) => {
                    const nextSlide = (prev + 1) % TOTAL_CAROUSEL_SLIDES;
                    if (carouselRef.current) {
                        carouselRef.current.goTo(nextSlide);
                    }
                    return nextSlide;
                });
            }, SLIDE_DURATION_MS);

            return () => {
                if (slideTimer.current) {
                    clearInterval(slideTimer.current);
                }
            };
        }

        return undefined;
    }, [TOTAL_CAROUSEL_SLIDES, shouldShow, videoSources]);

    const handleSlideChange = (current: number) => {
        // Prevent invalid slide changes during carousel initialization
        if (current >= 0 && current < TOTAL_CAROUSEL_SLIDES) {
            const direction = current > currentSlide ? 'forward' : 'backward';

            analytics.event({
                type: EventType.WelcomeToDataHubModalInteractEvent,
                direction,
                currentSlide: current + 1,
                totalSlides: TOTAL_CAROUSEL_SLIDES,
            });

            setCurrentSlide(current);
        }

        // Clear and restart timer on manual navigation
        if (slideTimer.current) {
            clearInterval(slideTimer.current);
        }

        slideTimer.current = setInterval(() => {
            setCurrentSlide((prev) => {
                const nextSlide = (prev + 1) % TOTAL_CAROUSEL_SLIDES;
                if (carouselRef.current) {
                    carouselRef.current.goTo(nextSlide);
                }
                return nextSlide;
            });
        }, SLIDE_DURATION_MS);

        return undefined;
    };

    function closeTour(
        exitMethod: 'close_button' | 'get_started_button' | 'outside_click' | 'escape_key' = 'close_button',
    ) {
        analytics.event({
            type: EventType.WelcomeToDataHubModalExitEvent,
            currentSlide: currentSlide + 1,
            totalSlides: TOTAL_CAROUSEL_SLIDES,
            exitMethod,
        });

        setShouldShow(false);
        setCurrentSlide(0); // Reset to first slide for next opening

        // Clean up timer
        if (slideTimer.current) {
            clearInterval(slideTimer.current);
        }

        // Close modal tour context if it was manually triggered
        if (isModalTourOpen) {
            closeModalTour();
        } else {
            // Only set localStorage for automatic first-time tours, not manual triggers
            localStorage.setItem(SKIP_ONBOARDING_TOUR_KEY, 'true');
        }
    }

    if (!shouldShow) return null;

    // Show loading state while videos are being loaded
    if (videoLoading || !videoSources) {
        return (
            <Modal
                title={WELCOME_TO_DATAHUB_MODAL_TITLE}
                width={MODAL_WIDTH}
                onCancel={() => closeTour('close_button')}
                buttons={[
                    {
                        text: 'Get Started',
                        variant: 'filled',
                        onClick: () => closeTour('get_started_button'),
                    },
                ]}
            >
                <SlideContainer>
                    <Heading type="h2">&nbsp;</Heading>
                    <VideoContainer>
                        <LoadingContainer width={MODAL_IMAGE_WIDTH}>Loading...</LoadingContainer>
                    </VideoContainer>
                </SlideContainer>
            </Modal>
        );
    }

    function trackExternalLinkClick(url: string): void {
        analytics.event({
            type: EventType.WelcomeToDataHubModalClickViewDocumentationEvent,
            url,
        });
    }

    return (
        <Modal title={WELCOME_TO_DATAHUB_MODAL_TITLE} width={MODAL_WIDTH} onCancel={() => closeTour('close_button')}>
            <Carousel
                ref={carouselRef}
                autoplay={false}
                afterChange={handleSlideChange}
                arrows={false}
                leftComponent={
                    currentSlide === TOTAL_CAROUSEL_SLIDES - 1 ? (
                        <StyledDocsLink
                            href={DATAHUB_DOCS_URL}
                            target="_blank"
                            rel="noopener noreferrer"
                            onClick={() => {
                                trackExternalLinkClick(DATAHUB_DOCS_URL);
                            }}
                        >
                            DataHub Docs
                        </StyledDocsLink>
                    ) : undefined
                }
                rightComponent={
                    currentSlide === TOTAL_CAROUSEL_SLIDES - 1 ? (
                        <Button
                            className="primary-button"
                            variant="filled"
                            onClick={() => closeTour('get_started_button')}
                        >
                            Get started
                        </Button>
                    ) : undefined
                }
            >
                <SlideContainer>
                    <Heading type="h2" size="lg" color="gray" colorLevel={600} weight="bold">
                        Find Any Asset, Anywhere
                    </Heading>
                    <Heading type="h3" size="md" color="gray" colorLevel={1700}>
                        Search datasets, models, dashboards, and more across your entire stack
                    </Heading>
                    <VideoContainer>
                        {videosReady.search ? (
                            <video
                                width={MODAL_IMAGE_WIDTH}
                                autoPlay
                                loop
                                muted
                                playsInline
                                style={{
                                    borderRadius: '8px',
                                    boxShadow: '0 4px 12px rgba(0, 0, 0, 0.15)',
                                }}
                            >
                                <source src={videoSources?.search} type="video/mp4" />
                            </video>
                        ) : (
                            <LoadingContainer width={MODAL_IMAGE_WIDTH}>Loading video...</LoadingContainer>
                        )}
                        {videoSources?.search && !videosReady.search && (
                            <video
                                width={MODAL_IMAGE_WIDTH}
                                autoPlay
                                loop
                                muted
                                playsInline
                                onCanPlay={() => handleVideoLoad('search')}
                                style={{
                                    opacity: 0,
                                    position: 'absolute',
                                    pointerEvents: 'none',
                                    top: 0,
                                    left: 0,
                                }}
                            >
                                <source src={videoSources.search} type="video/mp4" />
                            </video>
                        )}
                    </VideoContainer>
                </SlideContainer>
                <SlideContainer>
                    <Heading type="h2" size="lg" color="gray" colorLevel={600} weight="bold">
                        Understand Your Data&apos;s Origin
                    </Heading>
                    <Heading type="h3" size="md" color="gray" colorLevel={1700}>
                        See the full story of how your data was created and transformed
                    </Heading>
                    <VideoContainer>
                        {videosReady.lineage ? (
                            <video
                                width={MODAL_IMAGE_WIDTH}
                                autoPlay
                                loop
                                muted
                                playsInline
                                style={{
                                    borderRadius: '8px',
                                    boxShadow: '0 4px 12px rgba(0, 0, 0, 0.15)',
                                }}
                            >
                                <source src={videoSources?.lineage} type="video/mp4" />
                            </video>
                        ) : (
                            <LoadingContainer width={MODAL_IMAGE_WIDTH}>Loading video...</LoadingContainer>
                        )}
                        {videoSources?.lineage && !videosReady.lineage && (
                            <video
                                width={MODAL_IMAGE_WIDTH}
                                autoPlay
                                loop
                                muted
                                playsInline
                                onCanPlay={() => handleVideoLoad('lineage')}
                                style={{
                                    opacity: 0,
                                    position: 'absolute',
                                    pointerEvents: 'none',
                                    top: 0,
                                    left: 0,
                                }}
                            >
                                <source src={videoSources.lineage} type="video/mp4" />
                            </video>
                        )}
                    </VideoContainer>
                </SlideContainer>
                <SlideContainer>
                    <Heading type="h2" size="lg" color="gray" colorLevel={600} weight="bold">
                        Manage Breaking Changes Confidently
                    </Heading>
                    <Heading type="h3" size="md" color="gray" colorLevel={1700}>
                        Preview the full impact of schema and column changes
                    </Heading>
                    <VideoContainer>
                        {videosReady.impact ? (
                            <video
                                width={MODAL_IMAGE_WIDTH}
                                autoPlay
                                loop
                                muted
                                playsInline
                                style={{
                                    borderRadius: '8px',
                                    boxShadow: '0 4px 12px rgba(0, 0, 0, 0.15)',
                                }}
                            >
                                <source src={videoSources?.impact} type="video/mp4" />
                            </video>
                        ) : (
                            <LoadingContainer width={MODAL_IMAGE_WIDTH}>Loading video...</LoadingContainer>
                        )}
                        {videoSources?.impact && !videosReady.impact && (
                            <video
                                width={MODAL_IMAGE_WIDTH}
                                autoPlay
                                loop
                                muted
                                playsInline
                                onCanPlay={() => handleVideoLoad('impact')}
                                style={{
                                    opacity: 0,
                                    position: 'absolute',
                                    pointerEvents: 'none',
                                    top: 0,
                                    left: 0,
                                }}
                            >
                                <source src={videoSources.impact} type="video/mp4" />
                            </video>
                        )}
                    </VideoContainer>
                </SlideContainer>
                {videoSources.aiDocs && (
                    <SlideContainer>
                        <Heading type="h2" size="lg" color="gray" colorLevel={600} weight="bold">
                            Documentation Without the Work
                        </Heading>
                        <Heading type="h3" size="md" color="gray" colorLevel={1700}>
                            Save hours of manual work while improving discoverability
                        </Heading>
                        <VideoContainer>
                            {videosReady.aiDocs ? (
                                <video
                                    width={MODAL_IMAGE_WIDTH}
                                    autoPlay
                                    loop
                                    muted
                                    playsInline
                                    style={{
                                        borderRadius: '8px',
                                        boxShadow: '0 4px 12px rgba(0, 0, 0, 0.15)',
                                    }}
                                >
                                    <source src={videoSources?.aiDocs} type="video/mp4" />
                                </video>
                            ) : (
                                <LoadingContainer width={MODAL_IMAGE_WIDTH}>Loading video...</LoadingContainer>
                            )}
                            {videoSources?.aiDocs && !videosReady.aiDocs && (
                                <video
                                    width={MODAL_IMAGE_WIDTH}
                                    autoPlay
                                    loop
                                    muted
                                    playsInline
                                    onCanPlay={() => handleVideoLoad('aiDocs')}
                                    style={{
                                        opacity: 0,
                                        position: 'absolute',
                                        pointerEvents: 'none',
                                        top: 0,
                                        left: 0,
                                    }}
                                >
                                    <source src={videoSources.aiDocs} type="video/mp4" />
                                </video>
                            )}
                        </VideoContainer>
                    </SlideContainer>
                )}
                <SlideContainer>
                    <Heading type="h2" size="lg" color="gray" colorLevel={600} weight="bold">
                        Ready to Get Started?
                    </Heading>
                    <Heading type="h3" size="md" color="gray" colorLevel={1700}>
                        Explore our comprehensive documentation or jump right in and start discovering your data.
                    </Heading>
                    <LoadedImage
                        src={WELCOME_MODAL_HOME_SCREENSHOT_PATH}
                        alt={WELCOME_TO_DATAHUB_MODAL_TITLE}
                        width={MODAL_IMAGE_WIDTH}
                    />
                </SlideContainer>
            </Carousel>
        </Modal>
    );
};
