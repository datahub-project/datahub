import { Button, Carousel, Heading, LoadedImage, Modal } from '@components';
import React, { useEffect, useRef, useState } from 'react';

import analytics, { EventType } from '@app/analytics';
import { useOnboardingTour } from '@app/onboarding/OnboardingTourContext.hooks';
import { ANT_NOTIFICATION_Z_INDEX } from '@app/shared/constants';
import {
    LoadingContainer,
    SlideContainer,
    StyledDocsLink,
    VideoContainer,
    VideoSlide,
} from '@src/app/onboarding/WelcomeToDataHubModal.components';

import welcomeModalHomeScreenshot from '@images/welcome-modal-home-screenshot.png';

const SLIDE_DURATION_MS = 10000;
const DATAHUB_DOCS_URL = 'https://docs.datahub.com/docs/category/features';
const WELCOME_TO_DATAHUB_MODAL_TITLE = 'Welcome to DataHub';
const SKIP_WELCOME_MODAL_KEY = 'skipWelcomeModal';

interface VideoSources {
    search: string;
    lineage: string;
    impact: string;
    aiDocs?: string;
}

function checkShouldSkipWelcomeModal() {
    return localStorage.getItem(SKIP_WELCOME_MODAL_KEY) === 'true';
}

export const WelcomeToDataHubModal = () => {
    const [shouldShow, setShouldShow] = useState(false);
    const [currentSlide, setCurrentSlide] = useState(0);
    const [videoSources, setVideoSources] = useState<VideoSources | null>(null);
    const [videoLoading, setVideoLoading] = useState(false);
    const [videosReady, setVideosReady] = useState<{ [key in keyof VideoSources]?: boolean }>({});
    const hasTrackedView = useRef(false);
    const carouselRef = useRef<any>(null);
    const { isModalTourOpen, closeModalTour } = useOnboardingTour();
    const shouldSkipWelcomeModal = checkShouldSkipWelcomeModal();
    const isDocumentationSlideEnabled = false;
    const TOTAL_CAROUSEL_SLIDES = isDocumentationSlideEnabled ? 5 : 4;
    const MODAL_IMAGE_WIDTH_RAW = 620;
    const MODAL_IMAGE_WIDTH = `${MODAL_IMAGE_WIDTH_RAW}px`;
    const MODAL_WIDTH_NUM = MODAL_IMAGE_WIDTH_RAW + 45; // Add padding
    const MODAL_WIDTH = `${MODAL_WIDTH_NUM}px`;

    // Automatic tour for first-time home page visitors
    useEffect(() => {
        if (!shouldSkipWelcomeModal) {
            setShouldShow(true);
            setCurrentSlide(0);
        }
    }, [shouldSkipWelcomeModal]);

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
                    setVideoSources((prev) =>
                        prev ? { ...prev, [videoKey]: (module.default) } : prev,
                    );
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

    const handleSlideChange = (current: number) => {
        // Called after carousel animation completes
        if (current >= 0 && current < TOTAL_CAROUSEL_SLIDES) {
            analytics.event({
                type: EventType.WelcomeToDataHubModalInteractEvent,
                currentSlide: current + 1,
                totalSlides: TOTAL_CAROUSEL_SLIDES,
            });

            setCurrentSlide(current);
        }
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

        if (isModalTourOpen) {
            closeModalTour();
        } else {
            // Only set localStorage for automatic first-time tours, not manual triggers
            localStorage.setItem(SKIP_WELCOME_MODAL_KEY, 'true');
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
        <Modal
            title={WELCOME_TO_DATAHUB_MODAL_TITLE}
            width={MODAL_WIDTH}
            onCancel={() => closeTour('close_button')}
            buttons={[]}
            zIndex={ANT_NOTIFICATION_Z_INDEX + 2} // 2 higher because home settings button is 1 higher
        >
            <Carousel
                ref={carouselRef}
                autoplay
                autoplaySpeed={SLIDE_DURATION_MS}
                afterChange={handleSlideChange}
                arrows={false}
                animateDot
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
                infinite={false}
            >
                <SlideContainer>
                    <Heading type="h2" size="lg" color="gray" colorLevel={600} weight="bold">
                        Find Any Asset, Anywhere
                    </Heading>
                    <Heading type="h3" size="md" color="gray" colorLevel={1700}>
                        Search datasets, models, dashboards, and more across your entire stack
                    </Heading>
                    <VideoContainer>
                        <VideoSlide
                            videoSrc={videoSources?.search}
                            isReady={videosReady.search || false}
                            onVideoLoad={() => handleVideoLoad('search')}
                            width={MODAL_IMAGE_WIDTH}
                        />
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
                        <VideoSlide
                            videoSrc={videoSources?.lineage}
                            isReady={videosReady.lineage || false}
                            onVideoLoad={() => handleVideoLoad('lineage')}
                            width={MODAL_IMAGE_WIDTH}
                        />
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
                        <VideoSlide
                            videoSrc={videoSources?.impact}
                            isReady={videosReady.impact || false}
                            onVideoLoad={() => handleVideoLoad('impact')}
                            width={MODAL_IMAGE_WIDTH}
                        />
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
                            <VideoSlide
                                videoSrc={videoSources?.aiDocs}
                                isReady={videosReady.aiDocs || false}
                                onVideoLoad={() => handleVideoLoad('aiDocs')}
                                width={MODAL_IMAGE_WIDTH}
                            />
                        </VideoContainer>
                    </SlideContainer>
                )}
                <SlideContainer>
                    <Heading type="h2" size="lg" color="gray" colorLevel={600} weight="bold">
                        Ready to Get Started?
                    </Heading>
                    <Heading type="h3" size="md" color="gray" colorLevel={1700}>
                        Explore our comprehensive documentation or jump right in and start discovering your data
                    </Heading>
                    <LoadedImage
                        src={welcomeModalHomeScreenshot}
                        alt={WELCOME_TO_DATAHUB_MODAL_TITLE}
                        width={MODAL_IMAGE_WIDTH}
                    />
                </SlideContainer>
            </Carousel>
        </Modal>
    );
};
