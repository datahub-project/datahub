import { Button } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useVisibilityObserver } from '@app/entityV2/summary/documentation/useVisibilityObserver';

const MAX_VIEW_HEIGHT = 286;

const Wrapper = styled.div<{ expanded: boolean }>`
    max-height: ${({ expanded }) => (expanded ? 'none' : `${MAX_VIEW_HEIGHT}px`)};
    display: flex;
    flex-direction: column;
    transition: max-height 0.3s ease;
`;

const ContentContainer = styled.div<{ $expanded: boolean; $hasMore: boolean }>`
    overflow: ${({ $expanded }) => ($expanded ? 'visible' : 'hidden')};
    ${({ $expanded, $hasMore }) =>
        !$expanded &&
        $hasMore &&
        `
            -webkit-mask-image: linear-gradient(to bottom, rgba(0,0,0,1) 60%, rgba(0,0,0,0.5) 90%, rgba(0,0,0,0) 100%);
            mask-image: linear-gradient(to bottom, rgba(0,0,0,1) 60%, rgba(0,0,0,0.5) 90%, rgba(0,0,0,0) 100%);
  `}
`;

const StyledButton = styled(Button)`
    align-self: center;
    margin-bottom: 16px;
`;

interface Props {
    children: React.ReactNode;
}

export default function DescriptionViewer({ children }: Props) {
    const [isExpanded, setIsExpanded] = useState(false);

    const { elementRef, hasMore } = useVisibilityObserver(MAX_VIEW_HEIGHT, [children]);

    return (
        <Wrapper expanded={isExpanded}>
            <ContentContainer ref={elementRef} $expanded={isExpanded} $hasMore={hasMore}>
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
