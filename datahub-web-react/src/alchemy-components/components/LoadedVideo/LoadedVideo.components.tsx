import styled from 'styled-components';

import colors from '@components/theme/foundations/colors';

const STORY_WIDTH = '600px';
const STORY_PADDING = '20px';

export const StoryContainer = styled.div`
    width: ${STORY_WIDTH};
    padding: ${STORY_PADDING};
`;

export const StoryTitle = styled.h3`
    margin-bottom: 20px;
    text-align: center;
    color: ${colors.gray[600]};
    font-size: 18px;
    font-weight: 600;
`;

export const StoryDescription = styled.p`
    margin-bottom: 20px;
    text-align: center;
    color: ${colors.gray[1700]};
    font-size: 14px;
    line-height: 1.5;
`;

export const CarouselContainer = styled.div`
    border: 1px solid ${colors.gray[100]};
    border-radius: 8px;
    padding: 20px;
`;

export const SlideContainer = styled.div`
    text-align: center;
    padding: 20px 0;
`;

export const SlideTitle = styled.h4`
    margin-bottom: 16px;
    font-size: 16px;
    font-weight: 600;
    color: ${colors.gray[600]};
`;

export const VideoContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    margin: 0 auto;
`;

export const ErrorContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 400px;
    height: 225px;
    background-color: ${colors.red[0]};
    border: 2px dashed ${colors.red[500]};
    border-radius: 8px;
    margin: 0 auto;
    font-size: 16px;
    color: ${colors.red[500]};
    font-weight: 500;
`;

export const LoadingContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 400px;
    height: 225px;
    background-color: ${colors.gray[1500]};
    border: 2px dashed ${colors.gray[100]};
    border-radius: 8px;
    margin: 0 auto;
    font-size: 16px;
    color: ${colors.gray[1700]};
    font-weight: 500;
`;

export const LoadingOverlay = styled.div`
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    background-color: ${colors.gray[1500]};
    z-index: 1;
    border-radius: 4px;
`;

export const VideoPlayerContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    margin: 0 auto;
    border-radius: 8px;
    overflow: hidden;
`;

export const IndicatorContainer = styled.div`
    margin-top: 20px;
    text-align: center;
`;

export const IndicatorRow = styled.div`
    display: flex;
    justify-content: center;
    gap: 10px;
`;

export const IndicatorDot = styled.div<{ $isActive: boolean }>`
    width: 12px;
    height: 12px;
    border-radius: 50%;
    background-color: ${({ $isActive }) => ($isActive ? colors.primary[500] : colors.gray[100])};
`;

export const IndicatorText = styled.p`
    margin-top: 10px;
    font-size: 12px;
    color: ${colors.gray[1800]};
`;
