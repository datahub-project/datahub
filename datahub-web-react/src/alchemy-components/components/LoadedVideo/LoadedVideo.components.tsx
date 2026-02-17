import styled from 'styled-components';

const STORY_WIDTH = '600px';
const STORY_PADDING = '20px';

export const StoryContainer = styled.div`
    width: ${STORY_WIDTH};
    padding: ${STORY_PADDING};
`;

export const StoryTitle = styled.h3`
    margin-bottom: 20px;
    text-align: center;
    color: ${({ theme }) => theme.colors.text};
    font-size: 18px;
    font-weight: 600;
`;

export const StoryDescription = styled.p`
    margin-bottom: 20px;
    text-align: center;
    color: ${({ theme }) => theme.colors.textSecondary};
    font-size: 14px;
    line-height: 1.5;
`;

export const CarouselContainer = styled.div`
    border: 1px solid ${({ theme }) => theme.colors.border};
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
    color: ${({ theme }) => theme.colors.text};
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
    background-color: ${(props) => props.theme.colors.bgSurfaceError};
    border: 2px dashed ${({ theme }) => theme.colors.textError};
    border-radius: 8px;
    margin: 0 auto;
    font-size: 16px;
    color: ${({ theme }) => theme.colors.textError};
    font-weight: 500;
`;

export const LoadingContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 400px;
    height: 225px;
    background-color: ${({ theme }) => theme.colors.bgSurface};
    border: 2px dashed ${({ theme }) => theme.colors.border};
    border-radius: 8px;
    margin: 0 auto;
    font-size: 16px;
    color: ${({ theme }) => theme.colors.textSecondary};
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
    background-color: ${({ theme }) => theme.colors.bgSurface};
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
    background-color: ${({ $isActive, theme }) => ($isActive ? theme.colors.textBrand : theme.colors.border)};
`;

export const IndicatorText = styled.p`
    margin-top: 10px;
    font-size: 12px;
    color: ${({ theme }) => theme.colors.textTertiary};
`;
