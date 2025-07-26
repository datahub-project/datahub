import { Carousel as AntCarousel, CarouselProps as AntCarouselProps } from 'antd';
import React, { ReactNode, forwardRef } from 'react';
import styled from 'styled-components';

import colors from '@components/theme/foundations/colors';

const CarouselContainer = styled.div`
    position: relative;
`;

const StyledCarousel = styled(AntCarousel)`
    .slick-dots {
        display: flex !important;
        justify-content: center;
        align-items: center;
        width: auto !important;
        pointer-events: none; /* Allow clicks to pass through */

        li {
            pointer-events: auto; /* Re-enable clicks on individual dots */
            margin: 0 4px !important;
            width: 12px !important;
            height: 12px !important;
            display: flex !important;

            button {
                width: 12px !important;
                height: 12px !important;
                margin: 0 !important;
                padding: 0 !important;
                border-radius: 50%;
                background: rgba(0, 0, 0, 0.3);
                border: none;
                opacity: 0.6;
                transition:
                    background-color 0.2s ease,
                    opacity 0.2s ease;

                &:hover {
                    background: rgba(0, 0, 0, 0.7);
                    opacity: 1;
                }
            }

            &.slick-active button {
                background: ${colors.primary[600]};
                opacity: 1;
            }
        }
    }

    .slick-prev,
    .slick-next {
        width: 40px;
        height: 40px;
        z-index: 2;

        &:before {
            font-size: 20px;
            color: rgba(0, 0, 0, 0.6);
            transition: color 0.2s ease;
        }

        &:hover:before {
            color: ${colors.primary[600]};
        }
        margin: 0 10px;
    }

    .slick-slide {
        text-align: center;
        min-height: 200px;
        display: flex;
        align-items: center;
        justify-content: center;
        border-radius: 8px;
    }
`;

const RightComponentContainer = styled.div`
    position: absolute;
    bottom: -2px;
    right: 14px;
    display: flex;
    align-items: center;
    gap: 8px;
    z-index: 20; /* Higher than dots to ensure clicks work */
    pointer-events: auto; /* Ensure this container captures clicks */
`;

const LeftComponentContainer = styled.div`
    position: absolute;
    bottom: -2px;
    display: flex;
    align-items: center;
    gap: 8px;
    z-index: 20; /* Higher than dots to ensure clicks work */
    pointer-events: auto; /* Ensure this container captures clicks */
`;

interface CarouselProps extends AntCarouselProps {
    rightComponent?: ReactNode;
    leftComponent?: ReactNode;
}

export const Carousel = forwardRef<any, CarouselProps>(
    (
        {
            autoplay = false,
            autoplaySpeed = 5000,
            arrows = false,
            dots = true,
            rightComponent,
            leftComponent,
            ...props
        },
        ref,
    ) => {
        return (
            <CarouselContainer>
                <StyledCarousel
                    ref={ref}
                    autoplay={autoplay}
                    autoplaySpeed={autoplaySpeed}
                    arrows={arrows}
                    dots={dots}
                    {...props}
                />
                {leftComponent && <LeftComponentContainer>{leftComponent}</LeftComponentContainer>}
                {rightComponent && <RightComponentContainer>{rightComponent}</RightComponentContainer>}
            </CarouselContainer>
        );
    },
);
