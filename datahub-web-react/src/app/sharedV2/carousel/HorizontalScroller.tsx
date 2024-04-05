import React, { useCallback, useEffect, useRef, useState } from 'react';
import styled from 'styled-components';
import NavigateNextOutlinedIcon from '@mui/icons-material/NavigateNextOutlined';
import NavigateBeforeOutlinedIcon from '@mui/icons-material/NavigateBeforeOutlined';
import { REDESIGN_COLORS } from '../../entityV2/shared/constants';

const ScrollButton = styled.button<{ alwaysVisible: boolean; size: number }>`
    border: none;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    width: ${({ size }) => size + 6}px;
    height: ${({ size }) => size + 6}px;
    border-radius: 50%;
    color: ${REDESIGN_COLORS.BLACK};
    background-color: ${REDESIGN_COLORS.WHITE};
    transition: background-color 0.3s ease-in-out, color 0.3s ease-in-out;
    box-shadow: 0px 2px 4px rgba(0, 0, 0, 0.3);
    opacity: ${({ alwaysVisible }) => (alwaysVisible ? '1' : '0')};
    &:hover {
        color: ${REDESIGN_COLORS.WHITE};
        background-color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }
`;

const ScrollContainerWrapper = styled.div`
    overflow-x: auto;
    scrollbar-width: none;
    width: 100%;
`;

const Wrapper = styled.div`
    position: relative;
    &:hover ${ScrollButton} {
        opacity: 0.9;
    }
`;

const StyledNavigateBeforeOutlinedIcon = styled(NavigateBeforeOutlinedIcon)<{ buttonSize: number }>`
    font-size: ${(props) => props.buttonSize}px !important;
`;

const StyledNavigateNextOutlinedIcon = styled(NavigateNextOutlinedIcon)<{ buttonSize: number }>`
    font-size: ${(props) => props.buttonSize}px !important;
`;

type Props = {
    children: React.ReactNode;
    scrollDistance?: number;
    scrollButtonSize?: number;
    alwaysVisible?: boolean;
};

const HorizontalScroller: React.FC<Props> = ({
    children,
    scrollDistance = 150,
    scrollButtonSize = 14,
    alwaysVisible = false,
}) => {
    const contentRef = useRef<HTMLDivElement>(null);
    const [showScrollLeft, setShowScrollLeft] = useState(false);
    const [showScrollRight, setShowScrollRight] = useState(false);

    const updateScrollButtons = useCallback(() => {
        if (contentRef.current) {
            const { scrollLeft, scrollWidth, clientWidth } = contentRef.current;
            setShowScrollLeft(scrollLeft > 0);
            setShowScrollRight(scrollWidth > clientWidth + scrollLeft);
        }
    }, []);

    useEffect(() => {
        updateScrollButtons();
    }, [updateScrollButtons]);

    useEffect(() => {
        if (!contentRef.current) return () => {};
        const div = contentRef.current;
        new ResizeObserver(updateScrollButtons).observe(div);
        div.addEventListener('scroll', updateScrollButtons);
        return () => {
            div.removeEventListener('scroll', updateScrollButtons);
        };
    }, [updateScrollButtons]);

    const scrollLeft = () => {
        if (contentRef.current) {
            contentRef.current.scrollTo({
                left: contentRef.current.scrollLeft - scrollDistance,
                behavior: 'smooth',
            });
        }
    };

    const scrollRight = () => {
        if (contentRef.current) {
            contentRef.current.scrollTo({
                left: contentRef.current.scrollLeft + scrollDistance,
                behavior: 'smooth',
            });
        }
    };

    return (
        <Wrapper>
            {showScrollLeft && (
                <ScrollButton onClick={scrollLeft} alwaysVisible={alwaysVisible} size={scrollButtonSize}>
                    <StyledNavigateBeforeOutlinedIcon buttonSize={scrollButtonSize} />
                </ScrollButton>
            )}
            <ScrollContainerWrapper ref={contentRef}>{children}</ScrollContainerWrapper>
            {showScrollRight && (
                <ScrollButton onClick={scrollRight} alwaysVisible={alwaysVisible} size={scrollButtonSize}>
                    <StyledNavigateNextOutlinedIcon buttonSize={scrollButtonSize} />
                </ScrollButton>
            )}
        </Wrapper>
    );
};

export default HorizontalScroller;
