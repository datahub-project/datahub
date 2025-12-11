/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
