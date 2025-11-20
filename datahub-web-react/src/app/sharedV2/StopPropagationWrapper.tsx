import React from 'react';
import styled from 'styled-components';

const StyledWrapper = styled.span`
    width: max-content;
    display: flex;
`;

interface Props {
    children: React.ReactNode;
}

const StopPropagationWrapper = ({ children }: Props) => {
    return (
        <StyledWrapper
            onClick={(e) => e.stopPropagation()}
            role="button"
            tabIndex={0}
            onKeyDown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                    e.stopPropagation();
                }
            }}
        >
            {children}
        </StyledWrapper>
    );
};

export default StopPropagationWrapper;
