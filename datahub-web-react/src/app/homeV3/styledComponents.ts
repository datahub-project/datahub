import { Icon, colors } from '@components';
import styled from 'styled-components';

import VectorBackground from '@images/homepage-vector.svg?react';

export const PageWrapper = styled.div`
    width: 100%;
    height: 100%;
    overflow: hidden;
    display: flex;
    flex-direction: column;
    box-shadow: 0px 4px 8px 0px rgba(33, 23, 95, 0.08);
    position: relative;
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

export const ContentContainer = styled.div`
    z-index: 1;
    padding: 40px 40px 16px 40px;
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: flex-start;
    align-items: center;
    overflow: auto;
`;

export const CenteredContainer = styled.div`
    max-width: 1016px;
    width: 100%;
`;

export const ContentDiv = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
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
    min-height: 200px;
`;

export const EmptyContainer = styled.div`
    display: flex;
    height: 50%;
    width: 100%;
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
