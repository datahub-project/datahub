import { Spin } from 'antd';
import React from 'react';
import styled from 'styled-components';

/**
 * Container for individual carousel slides with centered content
 */
export const SlideContainer = styled.div`
    position: relative; /* Provide positioning context for absolutely positioned children */
    text-align: left;
    margin-bottom: 32px;
    min-height: 470px;
`;

/**
 * Container for video elements with centered alignment
 */
export const VideoContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    padding-top: 16px;
`;

/**
 * Styled loading container base
 */
const LoadingContainerBase = styled.div<{ width: string }>`
    width: ${(props) => props.width};
    height: 350px; /* Match video aspect ratio for 620px width */
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    background-color: ${(props) => props.theme.styles['background-light']};
    border: 2px dashed ${(props) => props.theme.styles['border-color-base']};
    border-radius: 8px;
    font-size: 16px;
    color: ${(props) => props.theme.styles['text-secondary']};
    font-weight: 500;
    margin: 0 auto;
    gap: 16px;
`;

/**
 * Loading state container with Spin component
 * @param width - CSS width value for the container
 * @param children - Optional loading text
 */
export const LoadingContainer: React.FC<{ width: string; children?: React.ReactNode }> = ({
    width,
    children = 'Loading video...',
}) => (
    <LoadingContainerBase width={width}>
        <Spin size="large" />
        {children}
    </LoadingContainerBase>
);

/**
 * Styled anchor for DataHub Docs link
 */
export const StyledDocsLink = styled.a`
    color: rgb(83, 63, 209);
    text-align: center;
    font-size: 14px;
    font-style: normal;
    font-weight: 650;
    line-height: normal;
    letter-spacing: -0.07px;
    text-decoration: none;
    cursor: pointer;
    border-radius: 4px;
    padding: 10px 12px;

    &:hover {
        background-color: rgb(249, 250, 252);
    }
`;
