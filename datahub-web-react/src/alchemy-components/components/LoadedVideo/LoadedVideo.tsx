import { Spin } from 'antd';
import React, { useEffect, useRef, useState } from 'react';

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
    const [isLoading, setIsLoading] = useState(true);
    const [hasError, setHasError] = useState(false);
    const internalVideoRef = useRef<HTMLVideoElement>(null);
    const currentVideoRef = videoRef || internalVideoRef;

    useEffect(() => {
        const video = currentVideoRef.current;
        if (!video) return undefined;

        const handleLoadedData = () => {
            setIsLoading(false);
            onVideoLoad?.();
        };

        const handleError = () => {
            setIsLoading(false);
            setHasError(true);
            onVideoError?.();
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
    }, [onVideoLoad, onVideoError, onVideoCanPlay, onVideoEnded, onVideoTimeUpdate, currentVideoRef]);

    if (hasError) {
        return <ErrorContainer>Failed to load video</ErrorContainer>;
    }

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
            {isLoading && (
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
                        display: isLoading ? 'none' : 'block',
                    }}
                >
                    <source src={mp4Src} type="video/mp4" />
                    {webmSrc && <source src={webmSrc} type="video/webm" />}
                    <track kind="captions" />
                    <p>
                        Your browser doesn&apos;t support HTML5 video. Here&apos;s a{' '}
                        <a href={mp4Src}>link to the video</a> instead.
                    </p>
                </video>
            </VideoPlayerContainer>
        </div>
    );
};
