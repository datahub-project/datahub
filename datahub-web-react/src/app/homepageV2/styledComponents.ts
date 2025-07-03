import { colors } from '@components';
import styled from 'styled-components';

import VectorBackground from '@images/homepage-vector.svg?react';

export const PageWrapper = styled.div`
    width: 100%;
    height: 100%;
    border-radius: 12px;
    overflow: hidden;
    display: flex;
    flex-direction: column;
    box-shadow: 0px 4px 8px 0px rgba(33, 23, 95, 0.08);
`;

export const HeaderWrapper = styled.div`
    height: 160px;
    width: 100%;
    overflow: hidden;
    background: linear-gradient(180deg, #f8fcff 0%, #fafafb 100%);
    border: 1px solid ${colors.gray[100]};
    border-radius: 12px 12px 0 0;
`;

export const ContentWrapper = styled.div`
    position: relative;
    width: 100%;
    flex: 1;
    overflow: hidden;
`;

export const ContentContainer = styled.div`
    position: relative; // to enable z-index
    padding: 40px 160px 16px 160px;
    background-color: ${colors.white};
    height: 100%;
    z-index: 1;
`;

export const StyledVectorBackground = styled(VectorBackground)`
    width: 100%;
    transform: rotate(0deg);
    position: absolute;
    background: linear-gradient(180deg, #fbfbff 0%, #fafafb 74.57%);
    z-index: 0;
    pointer-events: none;
`;
