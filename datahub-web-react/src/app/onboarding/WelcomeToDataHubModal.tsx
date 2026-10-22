import { Button, Carousel, LoadedImage, Modal } from '@components';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import { unstable_batchedUpdates } from 'react-dom';
import { useTranslation } from 'react-i18next';

import analytics, { EventType } from '@app/analytics';
import { useOnboardingTour } from '@app/onboarding/OnboardingTourContext.hooks';
import { ANT_NOTIFICATION_Z_INDEX } from '@app/shared/constants';
import { checkShouldSkipWelcomeModal, setSkipWelcomeModal } from '@app/shared/localStorageUtils';
import {
    SlideContainer,
    SlideDescription,
    SlideTitle,
    StyledDocsLink,
    TourSlides,
    VideoContainer,
    VideoSlide,
} from '@src/app/onboarding/WelcomeToDataHubModal.components';
import { videoKeyForSlide, videoSrcForSlide } from '@src/app/onboarding/WelcomeToDataHubModal.utils';

import aiDocumentationPoster from '@images/FTE-ai-documentation-poster.webp';
import impactPoster from '@images/FTE-impact-poster.webp';
import lineagePoster from '@images/FTE-lineage-poster.webp';
import searchPoster from '@images/FTE-search-poster.webp';
import welcomeModalHomeScreenshot from '@images/welcome-modal-home-screenshot.webp';

const SLIDE_DURATION_MS = 10000;
/** Give up and move on if a clip never starts or never ends. */
const SLIDE_FALLBACK_MS = 25000;
/** Extra hold after the reported duration, in case `ended` is late. */
const CLIP_END_GRACE_MS = 1500;
/** Start the next clip's URL fetch this long before the current one ends. */
const NEXT_VIDEO_PREFETCH_LEAD_MS = 2000;
const DATAHUB_DOCS_URL = 'https://docs.datahub.com/docs/category/features';

interface VideoSources {
    search: string;
    lineage: string;
    impact: string;
    aiDocs?: string;
}

function loadTourVideo(videoKey: keyof VideoSources): Promise<string> {
    const loaders: Record<keyof VideoSources, () => Promise<{ default: string }>> = {
        search: () => import('@images/FTE-search.mp4'),
        lineage: () => import('@images/FTE-lineage.mp4'),
        impact: () => import('@images/FTE-impact.mp4'),
        aiDocs: () => import('@images/FTE-ai-documentation.mp4'),
    };
    return loaders[videoKey]().then((module) => module.default);
}

export const WelcomeToDataHubModal = () => {
    const { t } = useTranslation('onboarding');
    const [shouldShow, setShouldShow] = useState(false);
    const [currentSlide, setCurrentSlide] = useState(0);
    const [videoSources, setVideoSources] = useState<VideoSources>({
        search: '',
        lineage: '',
        impact: '',
    });
    const hasTrackedView = useRef(false);
    const carouselRef = useRef<any>(null);
    const currentSlideRef = useRef(0);
    const advancedFromSlideRef = useRef<number | null>(null);
    const advanceTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    const prefetchedFromSlideRef = useRef<number | null>(null);
    const [prefetchSlide, setPrefetchSlide] = useState<number | null>(null);
    const [clipStarted, setClipStarted] = useState(false);
    const loadCancelledRef = useRef(false);
    currentSlideRef.current = currentSlide;
    const { isModalTourOpen, closeModalTour } = useOnboardingTour();
    const shouldSkipWelcomeModal = checkShouldSkipWelcomeModal();
    const isDocumentationSlideEnabled = false;
    const TOTAL_CAROUSEL_SLIDES = isDocumentationSlideEnabled ? 5 : 4;
    const MODAL_IMAGE_WIDTH_RAW = 620;
    const MODAL_IMAGE_WIDTH = `${MODAL_IMAGE_WIDTH_RAW}px`;
    const MODAL_WIDTH_NUM = MODAL_IMAGE_WIDTH_RAW + 45; // Add padding
    const MODAL_WIDTH = `${MODAL_WIDTH_NUM}px`;

    // HomePage keeps this modal mounted, so a prefetch index would otherwise
    // survive the slide change, the close, and the next Product Tour open.
    const clearPrefetch = useCallback(() => {
        prefetchedFromSlideRef.current = null;
        setPrefetchSlide(null);
    }, []);

    // Automatic tour for first-time home page visitors
    useEffect(() => {
        if (!shouldSkipWelcomeModal) {
            clearPrefetch();
            setShouldShow(true);
            setCurrentSlide(0);
        }
    }, [clearPrefetch, shouldSkipWelcomeModal]);

    // Manual tour trigger from Product Tour buttons
    useEffect(() => {
        if (isModalTourOpen) {
            clearPrefetch();
            setClipStarted(false);
            setShouldShow(true);
            setCurrentSlide(0);
        }
    }, [clearPrefetch, isModalTourOpen]);

    const clearAdvanceTimer = useCallback(() => {
        if (advanceTimerRef.current) {
            clearTimeout(advanceTimerRef.current);
            advanceTimerRef.current = null;
        }
    }, []);

    // The lineage clip's column view is at the end, so advance on `ended`, not a fixed timer.
    const goToNextSlide = useCallback(() => {
        const slide = currentSlideRef.current;
        if (advancedFromSlideRef.current === slide) return;
        if (slide >= TOTAL_CAROUSEL_SLIDES - 1) return;
        advancedFromSlideRef.current = slide;
        clearAdvanceTimer();
        carouselRef.current?.next();
    }, [TOTAL_CAROUSEL_SLIDES, clearAdvanceTimer]);

    const rememberVideo = useCallback((videoKey: keyof VideoSources) => {
        loadTourVideo(videoKey)
            .then((src) => {
                if (loadCancelledRef.current) return;
                setVideoSources((prev) => (prev[videoKey] === src ? prev : { ...prev, [videoKey]: src }));
            })
            .catch((error) => {
                console.error(`Failed to load ${videoKey} video:`, error);
            });
    }, []);

    const scheduleAdvance = useCallback(
        (delayMs: number) => {
            clearAdvanceTimer();
            advanceTimerRef.current = setTimeout(goToNextSlide, delayMs);
        },
        [clearAdvanceTimer, goToNextSlide],
    );

    // Only the visible slide plays. About 2s before it ends, the next slide gets
    // its MP4 URL with preload and no autoplay, so that file actually downloads.
    useEffect(() => {
        if (!shouldShow) return undefined;

        loadCancelledRef.current = false;
        // The lock records the slide we already left. Showing that index again
        // (a dot click back, or the tour reopening on it) has to be able to advance.
        if (advancedFromSlideRef.current === currentSlide) {
            advancedFromSlideRef.current = null;
        }
        const videoKey = videoKeyForSlide(currentSlide, isDocumentationSlideEnabled);
        if (videoKey) {
            rememberVideo(videoKey);
        }

        const isLastSlide = currentSlide >= TOTAL_CAROUSEL_SLIDES - 1;
        if (!isLastSlide) {
            scheduleAdvance(videoKey ? SLIDE_FALLBACK_MS : SLIDE_DURATION_MS);
        }

        return () => {
            loadCancelledRef.current = true;
            clearAdvanceTimer();
        };
    }, [
        TOTAL_CAROUSEL_SLIDES,
        clearAdvanceTimer,
        currentSlide,
        isDocumentationSlideEnabled,
        rememberVideo,
        scheduleAdvance,
        shouldShow,
    ]);

    function holdForRestOfClip(video: HTMLVideoElement) {
        if (advancedFromSlideRef.current === currentSlideRef.current) return;
        const remainingMs = (video.duration - video.currentTime) * 1000;
        if (!Number.isFinite(remainingMs) || remainingMs < 0) return;
        // Keep pushing the advance out to the end of this play. A stall
        // stops the updates, so the last schedule still moves us on.
        scheduleAdvance(remainingMs + CLIP_END_GRACE_MS);
    }

    function handleVideoPlaying(event: React.SyntheticEvent<HTMLVideoElement>) {
        setClipStarted(true);
        holdForRestOfClip(event.currentTarget);
    }

    function handleVideoTimeUpdate(event: React.SyntheticEvent<HTMLVideoElement>) {
        const video = event.currentTarget;
        holdForRestOfClip(video);
        const slide = currentSlideRef.current;
        if (prefetchedFromSlideRef.current === slide) return;
        if (!Number.isFinite(video.duration) || video.duration <= 0) return;
        const remainingMs = (video.duration - video.currentTime) * 1000;
        if (remainingMs > NEXT_VIDEO_PREFETCH_LEAD_MS) return;
        prefetchedFromSlideRef.current = slide;
        const nextSlide = slide + 1;
        const nextKey = videoKeyForSlide(nextSlide, isDocumentationSlideEnabled);
        if (!nextKey) return;
        rememberVideo(nextKey);
        setPrefetchSlide(nextSlide);
    }

    function handleVideoFinished() {
        goToNextSlide();
    }

    // A broken clip used to leave its poster up for the slide. Skipping on
    // `error` hides the title before anyone can read it.
    function handleVideoError() {
        setClipStarted(true);
        scheduleAdvance(SLIDE_DURATION_MS);
    }

    // A manual move owns the slide so a late `ended` from the clip we left
    // does not advance again. ArrowLeft on the first slide and ArrowRight on
    // the last are no-ops (`infinite` is off). Stamping the lock anyway, and
    // clearing the fallback timer, freezes that clip.
    function leaveSlide(direction: 'prev' | 'next') {
        const slide = currentSlideRef.current;
        const canMove = direction === 'prev' ? slide > 0 : slide < TOTAL_CAROUSEL_SLIDES - 1;
        if (!canMove) return;
        advancedFromSlideRef.current = slide;
        clearAdvanceTimer();
        if (direction === 'prev') carouselRef.current?.prev();
        else carouselRef.current?.next();
    }

    // Keyboard navigation: Left/Right cycle slides, Escape closes the modal.
    // Bound at document level because focus inside the modal sits on a non-carousel
    // element (close button / body), so react-slick's built-in handler never fires.
    useEffect(() => {
        if (!shouldShow) return undefined;

        function handleKeyDown(e: KeyboardEvent) {
            if (e.key === 'ArrowLeft') {
                e.preventDefault();
                leaveSlide('prev');
            } else if (e.key === 'ArrowRight') {
                e.preventDefault();
                leaveSlide('next');
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
        // react-slick calls this from a timer after the animation. React 17 does not
        // batch those updates, so clearing the prefetch index on its own drops the
        // next clip's URL while the carousel is still on the old slide and the
        // buffered <video> unmounts.
        if (current >= 0 && current < TOTAL_CAROUSEL_SLIDES) {
            analytics.event({
                type: EventType.WelcomeToDataHubModalInteractEvent,
                currentSlide: current + 1,
                totalSlides: TOTAL_CAROUSEL_SLIDES,
            });

            unstable_batchedUpdates(() => {
                setCurrentSlide(current);
                setClipStarted(false);
                clearPrefetch();
            });
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

        advancedFromSlideRef.current = null;
        clearPrefetch();
        setClipStarted(false);
        clearAdvanceTimer();
        setShouldShow(false);
        setCurrentSlide(0); // Reset to first slide for next opening

        if (isModalTourOpen) {
            closeModalTour();
        } else {
            // Only set localStorage for automatic first-time tours, not manual triggers
            setSkipWelcomeModal(true);
        }
    }

    if (!shouldShow) return null;

    function trackExternalLinkClick(url: string): void {
        analytics.event({
            type: EventType.WelcomeToDataHubModalClickViewDocumentationEvent,
            url,
        });
    }

    function tourVideo(slideIndex: number, src: string | undefined, posterSrc: string) {
        const active = currentSlide === slideIndex;
        return (
            <VideoSlide
                active={active}
                videoSrc={videoSrcForSlide(currentSlide, slideIndex, src, prefetchSlide)}
                posterSrc={posterSrc}
                width={MODAL_IMAGE_WIDTH}
                onPlaying={active ? handleVideoPlaying : undefined}
                onTimeUpdate={active ? handleVideoTimeUpdate : undefined}
                onEnded={active ? handleVideoFinished : undefined}
                onError={active ? handleVideoError : undefined}
            />
        );
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
            <TourSlides>
                <Carousel
                    ref={carouselRef}
                    autoplay={false}
                    afterChange={handleSlideChange}
                    arrows={false}
                    animateDot={!videoKeyForSlide(currentSlide, isDocumentationSlideEnabled) || clipStarted}
                    dotDuration={SLIDE_DURATION_MS}
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
                        <VideoContainer>{tourVideo(0, videoSources.search, searchPoster)}</VideoContainer>
                    </SlideContainer>
                    <SlideContainer>
                        <SlideTitle>{t('welcome.slideLineageTitle')}</SlideTitle>
                        <SlideDescription>{t('welcome.slideLineageDescription')}</SlideDescription>
                        <VideoContainer>{tourVideo(1, videoSources.lineage, lineagePoster)}</VideoContainer>
                    </SlideContainer>
                    <SlideContainer>
                        <SlideTitle>{t('welcome.slideImpactTitle')}</SlideTitle>
                        <SlideDescription>{t('welcome.slideImpactDescription')}</SlideDescription>
                        <VideoContainer>{tourVideo(2, videoSources.impact, impactPoster)}</VideoContainer>
                    </SlideContainer>
                    {isDocumentationSlideEnabled && (
                        <SlideContainer>
                            <SlideTitle>{t('welcome.slideDocsTitle')}</SlideTitle>
                            <SlideDescription>{t('welcome.slideDocsDescription')}</SlideDescription>
                            <VideoContainer>{tourVideo(3, videoSources.aiDocs, aiDocumentationPoster)}</VideoContainer>
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
            </TourSlides>
        </Modal>
    );
};
