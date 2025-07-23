import { Icon, colors } from '@components';
import styled from 'styled-components';

import VectorBackground from '@images/homepage-vector.svg?react';

export const PageWrapper = styled.div`
    width: 100%;
    height: 100%;
    overflow: auto;
    &::-webkit-scrollbar {
        display: none;
    }
    display: flex;
    flex-direction: column;
    box-shadow: 0px 4px 8px 0px rgba(33, 23, 95, 0.08);
    position: relative;
    align-items: center;
`;

export const HomePageContainer = styled.div`
    position: relative;
    flex: 1;
    overflow: hidden;
    margin: 5px;
`;

export const StyledVectorBackground = styled(VectorBackground)`
    position: absolute;
    width: 100%;
    height: 100%;
    z-index: 0;
    transform: rotate(0deg);
    pointer-events: none;
    border-radius: 12px;
    background-color: ${colors.white};
`;

export const contentWidth = (additionalWidth = 0) => `
    width: calc(75% + ${additionalWidth}px);

    @media (max-width: 1500px) {
        width: calc(85% + ${additionalWidth}px);
    }
    @media (max-width: 1250px) {
        width: 100%;
    }
`;

export const ContentContainer = styled.div`
    z-index: 1;
    padding: 24px 0 16px 0;
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: flex-start;
    align-items: center;
    ${contentWidth(0)}
`;

export const CenteredContainer = styled.div`
    max-width: 1600px; // could simply increase this - ask in design review
    width: 100%;
    padding: 0 8px;
`;

export const ContentDiv = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    overflow-y: auto;
`;

export const StyledIcon = styled(Icon)`
    :hover {
        cursor: pointer;
    }
`;

export const LoaderContainer = styled.div`
    display: flex;
    height: 100%;
    height: 200px;
`;

export const EmptyContainer = styled.div`
    display: flex;
    width: 100%;
    height: 200px;
    justify-content: center;
    align-items: center;
`;

export const FloatingRightHeaderSection = styled.div`
    position: absolute;
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 8px;
    padding-right: 16px;
    right: 0px;
    top: 0px;
    height: 100%;
`;
