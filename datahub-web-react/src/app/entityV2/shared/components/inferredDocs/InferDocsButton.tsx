import React from 'react';
import styled, { keyframes } from 'styled-components';
import { Sparkle } from 'phosphor-react';
import analytics, { EventType, InferDocsClickEvent } from '../../../../analytics';

const GradientAnimation = keyframes`
    0% {
        opacity: 0.4;
        transform: rotate(0deg);
        // background-position: 0% 50%;
    }
    50% {
        transform: rotate(180deg);
        opacity: 0.5;
        // background-position: 100% 50%;
    }
    100% {
        transform: rotate(360deg);
        opacity: 0.4;
        // background-position: 0% 50%;
    }
`;

const GenerateButton = styled.button`
    position: relative;
    background-color: #f1f3fd;
    width: 140px;
    height: 40px;
    display: flex;
    border-radius: 8px;
    overflow: hidden;
    cursor: pointer;
    border: 0;
    box-shadow: 0px 0px 8px #3628ff1a;
    font-size: 12px;
    &:hover,
    &:focus {
        box-shadow: none;
        border: 0;
        background-color: #f9f3fd;
        box-shadow: 0px 0px 16px #2897ff44;
    }
    transition: box-shadow ease 1s;
    span,
    path,
    line {
        color: #5c3fd1 !important;
    }
`;
const ButtonContent = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    z-index: 2;
    position: relative;
    align-self: center;
    width: 100%;
`;

const ButtonBackground = styled.div`
    position: absolute;
    top: 0;
    left: 0;
    height: 100%;
    width: 100%;
    z-index: 0;
    padding: 1.5px;
    ::before {
        content: '';
        position: absolute;
        width: 200%;
        height: 300px;
        top: -75px;
        left: -50%;
        z-index: -1;
        background: linear-gradient(-45deg, #7839ff, #ede4ff, #28b6ff, #99dbfd, #4e02f0, #a781f7, #3628ff, #7839ff);
        animation: ${GradientAnimation} 10s ease infinite;
        background-size: 100%;
    }
`;

const ButtonBackgroundInner = styled.div`
    height: 100%;
    width: 100%;
    background-color: #f1f3fd;
    opacity: 0.92;
    z-index: 1;
    border-radius: 6px;
`;

const AiSparkle = styled(Sparkle)`
    height: 14px;
    width: 14px;
    margin-right: 4px;
`;

type Props = {
    title?: string;
    onClick: () => void;
    style?: React.CSSProperties;
    // For analytics
    surface?: InferDocsClickEvent['surface'];
};

export default function InferDocsButton({ title = 'Generate with AI', onClick, surface, style }: Props) {
    const onClickInternal = () => {
        if (surface) {
            analytics.event({
                type: EventType.InferDocsClickEvent,
                surface,
            });
        }
        onClick();
    };

    return (
        <GenerateButton style={style} type="button" onClick={onClickInternal}>
            <ButtonContent>
                <AiSparkle />
                <span>{title}</span>
            </ButtonContent>
            <ButtonBackground>
                <ButtonBackgroundInner />
            </ButtonBackground>
        </GenerateButton>
    );
}
