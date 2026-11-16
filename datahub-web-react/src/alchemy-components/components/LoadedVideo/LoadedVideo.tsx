import { Spin } from 'antd';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';

import {
    ErrorContainer,
    LoadingOverlay,
    VideoPlayerContainer,
} from '@src/alchemy-components/components/LoadedVideo/LoadedVideo.components';

interface LoadedVideoProps {
    mp4Src: string;
    webmSrc?: string;
    posterSrc?: string;
    width?: string;
    height?: string;
    autoplay?: boolean;
    loop?: boolean;
    muted?: boolean;
    controls?: boolean;
    playsInline?: boolean;
    className?: string;
    onVideoLoad?: () => void;
    onVideoError?: () => void;
    onVideoEnded?: () => void;
    onVideoTimeUpdate?: (currentTime: number, duration: number) => void;
    onVideoCanPlay?: () => void;
    videoRef?: React.RefObject<HTMLVideoElement>;
}

export const LoadedVideo: React.FC<LoadedVideoProps> = ({
    mp4Src,
    webmSrc,
    posterSrc,
    width,
    height,
    autoplay = true,
    loop = true,
    muted = true,
    controls = false,
    playsInline = true,
    className,
    onVideoLoad,
    onVideoError,
    onVideoEnded,
    onVideoTimeUpdate,
    onVideoCanPlay,
    videoRef,
}) => {
    const { t } = useTranslation('alchemy');
    const [isLoading, setIsLoading] = useState(true);
    const [hasError, setHasError] = useState(false);
    const internalVideoRef = useRef<HTMLVideoElement>(null);
    const currentVideoRef = videoRef || internalVideoRef;

    const handleError = useCallback(() => {
        setIsLoading(false);
        setHasError(true);
        onVideoError?.();
    }, [onVideoError]);

    useEffect(() => {
        const video = currentVideoRef.current;
        if (!video) return undefined;

        const handleLoadedData = () => {
            setIsLoading(false);
            onVideoLoad?.();
        };

        const handleCanPlay = () => {
            onVideoCanPlay?.();
        };

        const handleEnded = () => {
            onVideoEnded?.();
        };

        const handleTimeUpdate = () => {
            if (onVideoTimeUpdate) {
                onVideoTimeUpdate(video.currentTime, video.duration);
            }
        };

        video.addEventListener('loadeddata', handleLoadedData);
        video.addEventListener('error', handleError);
        video.addEventListener('canplay', handleCanPlay);
        video.addEventListener('ended', handleEnded);
        video.addEventListener('timeupdate', handleTimeUpdate);

        return () => {
            video.removeEventListener('loadeddata', handleLoadedData);
            video.removeEventListener('error', handleError);
            video.removeEventListener('canplay', handleCanPlay);
            video.removeEventListener('ended', handleEnded);
            video.removeEventListener('timeupdate', handleTimeUpdate);
        };
    }, [onVideoLoad, handleError, onVideoCanPlay, onVideoEnded, onVideoTimeUpdate, currentVideoRef]);

    if (hasError) {
        return <ErrorContainer>{t('loadedVideo.error')}</ErrorContainer>;
    }

    // With a poster, show the video element immediately so the poster is visible while loading.
    const showLoadingOverlay = isLoading && !posterSrc;

    return (
        <div
            style={{
                position: 'relative',
                width,
                height,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                margin: '0 auto',
            }}
        >
            {showLoadingOverlay && (
                <LoadingOverlay>
                    <Spin size="large" />
                </LoadingOverlay>
            )}
            <VideoPlayerContainer>
                <video
                    ref={currentVideoRef}
                    width={width}
                    height={height}
                    autoPlay={autoplay}
                    loop={loop}
                    muted={muted}
                    controls={controls}
                    playsInline={playsInline}
                    poster={posterSrc}
                    className={className}
                    style={{
                        display: showLoadingOverlay ? 'none' : 'block',
                    }}
                >
                    {/* A failed source fires `error` on <source>, not <video>; only the last means all failed. */}
                    <source src={mp4Src} type="video/mp4" onError={webmSrc ? undefined : handleError} />
                    {webmSrc && <source src={webmSrc} type="video/webm" onError={handleError} />}
                    <track kind="captions" />
                    <p>
                        <Trans
                            t={t}
                            i18nKey="loadedVideo.unsupportedFallback"
                            components={{
                                // eslint-disable-next-line jsx-a11y/anchor-has-content, jsx-a11y/control-has-associated-label
                                anchor: <a href={mp4Src} />,
                            }}
                        />
                    </p>
                </video>
            </VideoPlayerContainer>
        </div>
    );
};
