import styled from 'styled-components';

const STORY_WIDTH = '600px';
const STORY_PADDING = '20px';
const VIDEO_WIDTH = '520px';

export const StoryContainer = styled.div`
    width: ${STORY_WIDTH};
    padding: ${STORY_PADDING};
`;

export const StoryTitle = styled.h3`
    margin-bottom: 20px;
    text-align: center;
    color: #262626;
    font-size: 18px;
    font-weight: 600;
`;

export const StoryDescription = styled.p`
    margin-bottom: 20px;
    text-align: center;
    color: #666;
    font-size: 14px;
    line-height: 1.5;
`;

export const CarouselContainer = styled.div`
    border: 1px solid #e0e0e0;
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
    color: #262626;
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
    background-color: #fff2f0;
    border: 2px dashed #ff4d4f;
    border-radius: 8px;
    margin: 0 auto;
    font-size: 16px;
    color: #ff4d4f;
    font-weight: 500;
`;

export const LoadingContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 400px;
    height: 225px;
    background-color: #f5f5f5;
    border: 2px dashed #d9d9d9;
    border-radius: 8px;
    margin: 0 auto;
    font-size: 16px;
    color: #8c8c8c;
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
    background-color: #f5f5f5;
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
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
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
    background-color: ${({ $isActive }) => ($isActive ? '#1890ff' : '#d9d9d9')};
`;

export const IndicatorText = styled.p`
    margin-top: 10px;
    font-size: 12px;
    color: #999;
`;

export { VIDEO_WIDTH };
