import { Button } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import colors from '@src/alchemy-components/theme/foundations/colors';

const HeaderContainer = styled.div<{ $isCollapsed: boolean }>`
    position: relative;
    margin-top: 8px;
    padding: 8px 0;
    color: #8088a3;
    font-family: Mulish;
    font-size: 14px;
    font-style: normal;
    font-weight: 700;
    line-height: normal;
    min-height: 38px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    cursor: default;

    @media (max-height: 970px) {
        margin-top: 2px;
    }
    @media (max-height: 890px) {
        margin-top: 0px;
    }
    @media (max-height: 835px) {
        min-height: 34px;
    }
    @media (max-height: 800px) {
        min-height: 24px;
    }
    @media (max-height: 775px) {
        min-height: 14px;
    }
    @media (max-height: 750px) {
        min-height: 0px;
        padding: 4px 0;
    }
    @media (max-height: 730px) {
        min-height: 0px;
        padding: 0;
    }
`;

const Title = styled.div`
    flex: 1;
`;

const PlusButton = styled(Button)<{ $show: boolean }>`
    display: ${(props) => (props.$show ? 'flex' : 'none')};
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    border: none;
    border-radius: 4px;
    background: transparent;
    color: ${colors.gray[1800]};
    cursor: pointer;
    transition: all 0.2s ease;
    padding: 0;
    margin: 0;

    &:hover {
        background: ${colors.violet[0]};
        color: ${colors.violet[600]};
    }

    &:active {
        transform: scale(0.95);
    }

    svg {
        width: 16px;
        height: 16px;
    }
`;

interface Props {
    title: string;
    isCollapsed: boolean;
    onAddClick: () => void;
    isLoading?: boolean;
}

export const ContextGroupHeader: React.FC<Props> = ({ title, isCollapsed, onAddClick, isLoading }) => {
    const [isHovered, setIsHovered] = useState(false);

    if (isCollapsed) {
        return null;
    }

    return (
        <HeaderContainer
            $isCollapsed={isCollapsed}
            onMouseEnter={() => setIsHovered(true)}
            onMouseLeave={() => setIsHovered(false)}
        >
            <Title>{title}</Title>
            <PlusButton
                $show={isHovered && !isLoading}
                onClick={onAddClick}
                aria-label="Add new document"
                disabled={isLoading}
                icon={{ icon: 'Plus', source: 'phosphor' }}
                isCircle
                variant="text"
            />
        </HeaderContainer>
    );
};
