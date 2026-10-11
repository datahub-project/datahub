import { Icon, Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import { TextProps } from '@components/components/Text/types';

import VectorBackground from '@images/homepage-vector.svg?react';

export const PageWrapper = styled.div`
    width: 100%;
    height: 100%;
    overflow: auto;
    &::-webkit-scrollbar {
        display: none;
    }
    display: flex;
    flex-direction: column;
    align-items: center;
`;

export const HomePageContainer = styled.div`
    position: relative;
    flex: 1;
    overflow: hidden;
    margin: 5px;
    border-radius: 12px;
    box-shadow: ${(props) => props.theme.colors.shadowLg};
    background-color: ${(props) => props.theme.colors.bg};
`;

export const StyledVectorBackground = styled(VectorBackground)`
    position: absolute;
    width: 100%;
    height: 100%;
    z-index: 0;
    transform: rotate(0deg);
    pointer-events: none;
    border-radius: 12px;
    background-color: ${(props) => props.theme.colors.bg};
    ${(props) => props.theme.id === 'themeV2Dark' && 'opacity: 0;'}
`;

export const contentWidth = (additionalWidth = 0) => `
    width: calc(75% + ${additionalWidth}px);

    @media (max-width: 1500px) {
        width: calc(85% + ${additionalWidth}px);
    }
    @media (max-width: 1250px) {
        width: 100%;
    }
`;

export const ContentContainer = styled.div`
    z-index: 1;
    padding: 24px 0 16px 0;
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: flex-start;
    align-items: center;
    ${contentWidth(0)}
`;

export const CenteredContainer = styled.div`
    max-width: 1600px; // could simply increase this - ask in design review
    width: 100%;
    padding: 0 8px 16px 8px;
`;

export const ContentDiv = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    overflow-y: auto;
`;

export const StyledIcon = styled(Icon)`
    :hover {
        cursor: pointer;
    }
`;

export const LoaderContainer = styled.div`
    display: flex;
    height: 100%;
    height: 200px;
`;

export const EmptyContainer = styled.div`
    display: flex;
    width: 100%;
    height: 200px;
    justify-content: center;
    align-items: center;
`;

export const FloatingRightHeaderSection = styled.div`
    position: absolute;
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 8px;
    padding-right: 16px;
    right: 0px;
    top: 0px;
    height: 100%;
`;

type EllipsisTextProps = TextProps & {
    ellipsis?: {
        tooltip?: {
            showArrow?: boolean;
            color?: string;
            overlayInnerStyle?: React.CSSProperties;
        };
    };
};

function EllipsisText({ ellipsis, children, ...props }: EllipsisTextProps) {
    const textRef = React.useRef<HTMLSpanElement>(null);
    const [isTruncated, setIsTruncated] = React.useState(false);
    const htmlProps = { ...props };
    delete htmlProps.color;
    delete htmlProps.size;
    delete htmlProps.weight;
    delete htmlProps.type;
    delete htmlProps.theme;

    React.useEffect(() => {
        const element = textRef.current;
        if (!element) return undefined;

        const updateTruncation = () => setIsTruncated(element.scrollWidth > element.clientWidth);
        updateTruncation();

        const observer = typeof ResizeObserver === 'undefined' ? undefined : new ResizeObserver(updateTruncation);
        observer?.observe(element);
        window.addEventListener('resize', updateTruncation);

        return () => {
            observer?.disconnect();
            window.removeEventListener('resize', updateTruncation);
        };
    }, [children]);

    const text = React.createElement('span', { ...htmlProps, ref: textRef }, children);

    return ellipsis
        ? React.createElement(
              Tooltip,
              {
                  title: isTruncated ? children : undefined,
                  showArrow: ellipsis.tooltip?.showArrow,
                  color: ellipsis.tooltip?.color,
                  overlayInnerStyle: ellipsis.tooltip?.overlayInnerStyle,
              },
              text,
          )
        : text;
}

export const NameContainer = styled(EllipsisText)`
    color: ${(props) => props.theme.colors.text};
    font-weight: 700;
    font-size: 16px;
    line-height: 20px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

export const DescriptionContainer = styled(EllipsisText)`
    color: ${(props) => props.theme.colors.textSecondary};
    font-weight: 400;
    font-size: 12px;
    line-height: 20px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;
