import React from 'react';
import styled, { keyframes } from 'styled-components';
import { Sparkle } from 'phosphor-react';

const GradientAnimation = keyframes`
    0% {
        opacity: 0.3;
        background-position: 0% 50%;
    }
    50% {
        opacity: 0.6;
        background-position: 100% 50%;
        background-rotation: 45deg;
    }
    100% {
        opacity: 0.3;
        background-position: 0% 50%;
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
    box-shadow: none;
    font-size: 12px;
    &:hover,
    &:focus {
        box-shadow: none;
        border: 0;
        background-color: #f9f3fd;
    }
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
    background: linear-gradient(-45deg, #7839ff, #99dbfd, #4e02f0, #99dbfd);
    animation: ${GradientAnimation} 10s ease infinite;
    background-size: 400%;
    z-index: 0;
    padding: 1.5px;
`;

const ButtonBackgroundInner = styled.div`
    height: 100%;
    width: 100%;
    background-color: #f1f3fd;
    opacity: 0.9;
    z-index: 1;
    border-radius: 6px;
`;

const AiSparkle = styled(Sparkle)`
    height: 14px;
    width: 14px;
    margin-right: 4px;
`;

type Props = {
    onClick: () => void;
    style?: React.CSSProperties;
};

export default function InferDocsButton({ onClick, style }: Props) {
    return (
        <GenerateButton style={style} type="button" onClick={onClick}>
            <ButtonContent>
                <AiSparkle />
                <span>Generate with AI</span>
            </ButtonContent>
            <ButtonBackground>
                <ButtonBackgroundInner />
            </ButtonBackground>
        </GenerateButton>
    );
}
