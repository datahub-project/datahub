import { Button } from '@components';
import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

const MAX_VIEW_HEIGHT = 286;

const Wrapper = styled.div<{ expanded: boolean }>`
    max-height: ${({ expanded }) => (expanded ? 'none' : `${MAX_VIEW_HEIGHT}px`)};
    display: flex;
    flex-direction: column;
    transition: max-height 0.3s ease;
`;

const ContentContainer = styled.div<{ expanded: boolean }>`
    overflow: ${({ expanded }) => (expanded ? 'visible' : 'hidden')};
    ${({ expanded }) =>
        !expanded &&
        `
            -webkit-mask-image: linear-gradient(to bottom, rgba(0,0,0,1) 60%, rgba(0,0,0,0.5) 90%, rgba(0,0,0,0) 100%);
            mask-image: linear-gradient(to bottom, rgba(0,0,0,1) 60%, rgba(0,0,0,0.5) 90%, rgba(0,0,0,0) 100%);
  `}
`;

const StyledButton = styled(Button)`
    align-self: center;
`;

interface Props {
    children: React.ReactNode;
}

export default function DescriptionViewer({ children }: Props) {
    const contentRef = useRef<HTMLDivElement | null>(null);
    const [isExpanded, setIsExpanded] = useState(false);
    const [hasMore, setHasMore] = useState(false);

    useEffect(() => {
        const element = contentRef.current;
        if (element) {
            setHasMore(element.scrollHeight > MAX_VIEW_HEIGHT);
        }
    }, [children]);

    return (
        <Wrapper expanded={isExpanded}>
            <ContentContainer ref={contentRef} expanded={isExpanded}>
                {children}
            </ContentContainer>
            {hasMore && (
                <StyledButton
                    variant="text"
                    onClick={(e) => {
                        e.stopPropagation();
                        setIsExpanded((val) => !val);
                    }}
                >
                    {isExpanded ? 'View Less' : 'View More'}
                </StyledButton>
            )}
        </Wrapper>
    );
}
