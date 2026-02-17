import { Icon, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

const ButtonWrapper = styled.div<{ $visible: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    background-color: ${colors.white};
    height: 40px;
    width: 40px;
    position: fixed;
    right: 32px;
    bottom: 32px;
    border-radius: 200px;
    box-shadow: 0px 4px 12px 0px rgba(9, 1, 61, 0.12);
    transform: ${({ $visible }) => ($visible ? 'scale(1) translateY(0)' : 'scale(0) translateY(20px)')};
    opacity: ${({ $visible }) => ($visible ? 1 : 0)};
    pointer-events: ${({ $visible }) => ($visible ? 'auto' : 'none')};
    transition:
        transform 0.4s ease,
        opacity 0.3s ease;

    :hover {
        cursor: pointer;
    }
`;

interface Props {
    isVisible: boolean;
    onButtonClick: () => void;
}

export default function FloatingChatButton({ isVisible, onButtonClick }: Props) {
    return (
        <ButtonWrapper $visible={isVisible} onClick={onButtonClick}>
            <Icon icon="Sparkle" source="phosphor" color="primary" weight="fill" />
        </ButtonWrapper>
    );
}
