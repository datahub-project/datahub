import { Button, Popover, Tooltip } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import colors from '@src/alchemy-components/theme/foundations/colors';

const ButtonsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const HeaderContainer = styled.div`
    position: relative;
    padding: 0px 0px 0px 0px;
    color: #8088a3;
    font-family: Mulish;
    font-size: 14px;
    font-style: normal;
    font-weight: 700;
    line-height: normal;
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

const IconButton = styled(Button)<{ $show: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 24px;
    border: none;
    border-radius: 4px;
    background: transparent;
    color: ${colors.gray[1800]};
    cursor: pointer;
    transition: all 0.2s ease;
    padding: 0;
    margin: 0;
    opacity: ${(props) => (props.$show ? 1 : 0)};
    visibility: ${(props) => (props.$show ? 'visible' : 'hidden')};
    pointer-events: ${(props) => (props.$show ? 'auto' : 'none')};

    &:hover {
        background: ${colors.gray[100]};
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
    onAddClick: () => void;
    onSearchClick?: () => void;
    isLoading?: boolean;
    searchPopoverContent?: React.ReactNode;
    showSearchPopover?: boolean;
    onSearchPopoverChange?: (open: boolean) => void;
}

export const ContextGroupHeader: React.FC<Props> = ({
    title,
    onAddClick,
    onSearchClick,
    isLoading,
    searchPopoverContent,
    showSearchPopover,
    onSearchPopoverChange,
}) => {
    const [isHovered, setIsHovered] = useState(false);

    return (
        <HeaderContainer onMouseEnter={() => setIsHovered(true)} onMouseLeave={() => setIsHovered(false)}>
            <Title>{title}</Title>
            <ButtonsContainer>
                {onSearchClick && searchPopoverContent && (
                    <Popover
                        open={showSearchPopover}
                        onOpenChange={onSearchPopoverChange}
                        content={searchPopoverContent}
                        placement="rightTop"
                        overlayStyle={{ padding: 0 }}
                        overlayInnerStyle={{
                            padding: 0,
                            background: 'transparent',
                            boxShadow: 'none',
                        }}
                    >
                        <Tooltip title="Search context documents" placement="bottom">
                            <IconButton
                                data-testid="search-document-button"
                                $show={isHovered && !isLoading}
                                onClick={onSearchClick}
                                aria-label="Search context documents"
                                disabled={isLoading}
                                icon={{ icon: 'MagnifyingGlass', source: 'phosphor' }}
                                isCircle
                                variant="text"
                            />
                        </Tooltip>
                    </Popover>
                )}
                <Tooltip title="New context document" placement="bottom">
                    <IconButton
                        data-testid="create-document-button"
                        $show={isHovered && !isLoading}
                        onClick={onAddClick}
                        aria-label="Add new document"
                        disabled={isLoading}
                        icon={{ icon: 'Plus', source: 'phosphor' }}
                        isCircle
                        variant="text"
                    />
                </Tooltip>
            </ButtonsContainer>
        </HeaderContainer>
    );
};
