import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import NavigateNextOutlinedIcon from '@mui/icons-material/NavigateNextOutlined';
import NavigateBeforeOutlinedIcon from '@mui/icons-material/NavigateBeforeOutlined';
import styled from 'styled-components';
import { REDESIGN_COLORS, SEARCH_COLORS } from '../../entityV2/shared/constants';
import { HorizontalList } from '../../entityV2/shared/summary/ListComponents';

const Wrapper = styled.div`
    position: relative;

    :hover {
        .scroll-btn {
            display: flex;
            cursor: pointer;
        }
    }
`;

const CarouselHorizontalList = styled(HorizontalList)<{ hideMask?: boolean }>`
    align-items: center;
    padding-right: 0;
    ${({ hideMask }) => hideMask && 'mask-image: none;'}
`;

const ButtonContainer = styled.div<{ left?: boolean; right?: boolean }>`
    display: none;
    align-items: center;
    justify-content: center;
    position: absolute;

    margin: 0;
    padding: 0;
    width: 30px;
    height: 30px;

    top: 50%;
    transform: translateY(-50%);
    z-index: 2;

    border-radius: 100px;
    box-shadow: 0px 2px 4px rgba(0, 0, 0, 0.1);
    color: ${REDESIGN_COLORS.BLACK};
    background-color: ${REDESIGN_COLORS.WHITE};

    :hover {
        color: ${REDESIGN_COLORS.WHITE};
        background-color: ${SEARCH_COLORS.TITLE_PURPLE};
    }

    ${({ left }) => left && 'left: -10px;'}
    ${({ right }) => right && 'right: -10px;'}
`;

const NavigateBeforeOutlinedIconStyle = styled(NavigateBeforeOutlinedIcon)`
    font-size: 14px !important;
`;
const NavigateNextOutlinedIconStyle = styled(NavigateNextOutlinedIcon)`
    font-size: 14px !important;
`;

type Props = {
    children: React.ReactNode;
    className?: string;
};

export function Carousel({ children, className }: Props) {
    const [showPrevButton, setShowPrevButton] = useState<boolean>(false);
    const [showNextButton, setShowNextButton] = useState<boolean>(false);
    const scrollRef = useRef<HTMLDivElement>(null);
    const arrayOfChildren = React.Children.toArray(children);

    const numChildren = arrayOfChildren.length;
    const childRefs = useMemo(
        () => Array.from({ length: numChildren }).map(() => React.createRef<HTMLDivElement>()),
        [numChildren],
    );

    const handleScroll = useCallback(() => {
        if (!scrollRef.current) return;
        setShowPrevButton(scrollRef.current.scrollLeft > 0);
        setShowNextButton(
            scrollRef.current.scrollLeft + scrollRef.current.clientWidth + 1 < scrollRef.current.scrollWidth,
        );
    }, []);

    useEffect(() => {
        handleScroll();
    }, [handleScroll]);

    useEffect(() => {
        if (!scrollRef.current) return () => {};
        const div = scrollRef.current;
        new ResizeObserver(handleScroll).observe(div);
        div.addEventListener('scroll', handleScroll);
        return () => {
            div.removeEventListener('scroll', handleScroll);
        };
    }, [handleScroll]);

    const next = useCallback(() => {
        if (scrollRef.current) {
            const scrollDiv = scrollRef.current;
            const nextRef = childRefs.find(
                (ref) =>
                    ref.current &&
                    ref.current.offsetLeft + ref.current.scrollWidth > scrollDiv.scrollLeft + scrollDiv.clientWidth,
            );

            if (nextRef?.current) {
                nextRef.current.scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'start' });
            } else {
                scrollDiv.scrollTo({
                    left: scrollDiv.offsetLeft + scrollDiv.clientWidth,
                    behavior: 'smooth',
                });
            }
        }
    }, [childRefs]);

    const prev = useCallback(() => {
        if (scrollRef.current) {
            const scrollDiv = scrollRef.current;
            const prevRef = childRefs.find(
                (ref) => ref.current && ref.current.offsetLeft > scrollDiv.scrollLeft - scrollDiv.clientWidth,
            );

            if (prevRef?.current) {
                prevRef.current.scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'start' });
            } else {
                scrollDiv.scrollTo({
                    left: scrollDiv.offsetLeft - scrollDiv.clientWidth,
                    behavior: 'smooth',
                });
            }
        }
    }, [childRefs]);

    return (
        <Wrapper>
            {showPrevButton && (
                <ButtonContainer className="scroll-btn" left onClick={prev}>
                    <NavigateBeforeOutlinedIconStyle />
                </ButtonContainer>
            )}
            {showNextButton && (
                <ButtonContainer className="scroll-btn" right onClick={next}>
                    <NavigateNextOutlinedIconStyle />
                </ButtonContainer>
            )}
            <CarouselHorizontalList ref={scrollRef} hideMask={!showNextButton} className={className}>
                {arrayOfChildren.map((child, index) => (
                    <div ref={childRefs[index]}>{child}</div>
                ))}
            </CarouselHorizontalList>
        </Wrapper>
    );
}
