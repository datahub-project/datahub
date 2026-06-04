import { Button, Carousel, LoadedImage, Modal } from '@components';
import React, { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';

import analytics, { EventType } from '@app/analytics';
import { useOnboardingTour } from '@app/onboarding/OnboardingTourContext.hooks';
import { ANT_NOTIFICATION_Z_INDEX } from '@app/shared/constants';
import {
    LoadingContainer,
    SlideContainer,
    SlideDescription,
    SlideTitle,
    StyledDocsLink,
    VideoContainer,
    VideoSlide,
} from '@src/app/onboarding/WelcomeToDataHubModal.components';

import welcomeModalHomeScreenshot from '@images/welcome-modal-home-screenshot.png';

const SLIDE_DURATION_MS = 10000;
const DATAHUB_DOCS_URL = 'https://docs.datahub.com/docs/category/features';
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
    const { t } = useTranslation('onboarding');
    const { t: tf } = useTranslation('common.feedback');
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

    // Keyboard navigation: Left/Right cycle slides, Escape closes the modal.
    // Bound at document level because focus inside the modal sits on a non-carousel
    // element (close button / body), so react-slick's built-in handler never fires.
    useEffect(() => {
        if (!shouldShow) return undefined;

        function handleKeyDown(e: KeyboardEvent) {
            if (e.key === 'ArrowLeft') {
                e.preventDefault();
                carouselRef.current?.prev();
            } else if (e.key === 'ArrowRight') {
                e.preventDefault();
                carouselRef.current?.next();
            } else if (e.key === 'Escape') {
                e.preventDefault();
                closeTour('escape_key');
            }
        }

        document.addEventListener('keydown', handleKeyDown);
        return () => document.removeEventListener('keydown', handleKeyDown);
        // closeTour is stable for the lifetime of an open modal; depending on currentSlide
        // would re-bind on every slide change without changing behavior.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [shouldShow]);

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
                title={t('welcome.modalTitle')}
                width={MODAL_WIDTH}
                onCancel={() => closeTour('close_button')}
                keyboard={false}
                buttons={[
                    {
                        text: t('welcome.getStarted'),
                        variant: 'filled',
                        onClick: () => closeTour('get_started_button'),
                    },
                ]}
            >
                <SlideContainer>
                    <SlideTitle>&nbsp;</SlideTitle>
                    <VideoContainer>
                        <LoadingContainer width={MODAL_IMAGE_WIDTH}>{tf('loading')}</LoadingContainer>
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
            title={t('welcome.modalTitle')}
            width={MODAL_WIDTH}
            onCancel={() => closeTour('close_button')}
            keyboard={false}
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
                            {t('welcome.docsLink')}
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
                            {t('welcome.getStarted')}
                        </Button>
                    ) : undefined
                }
                infinite={false}
            >
                <SlideContainer>
                    <SlideTitle>{t('welcome.slideFindTitle')}</SlideTitle>
                    <SlideDescription>{t('welcome.slideFindDescription')}</SlideDescription>
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
                    <SlideTitle>{t('welcome.slideLineageTitle')}</SlideTitle>
                    <SlideDescription>{t('welcome.slideLineageDescription')}</SlideDescription>
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
                    <SlideTitle>{t('welcome.slideImpactTitle')}</SlideTitle>
                    <SlideDescription>{t('welcome.slideImpactDescription')}</SlideDescription>
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
                        <SlideTitle>{t('welcome.slideDocsTitle')}</SlideTitle>
                        <SlideDescription>{t('welcome.slideDocsDescription')}</SlideDescription>
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
                    <SlideTitle>{t('welcome.slideReadyTitle')}</SlideTitle>
                    <SlideDescription>{t('welcome.slideReadyDescription')}</SlideDescription>
                    <LoadedImage
                        src={welcomeModalHomeScreenshot}
                        alt={t('welcome.modalTitle')}
                        width={MODAL_IMAGE_WIDTH}
                    />
                </SlideContainer>
            </Carousel>
        </Modal>
    );
};
