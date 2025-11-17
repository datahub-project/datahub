import { Sparkle } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components';

import { getColor } from '@components/theme/utils';

const IconWrapper = styled.div`
    width: 20px;
    height: 20px;
    display: flex;
    align-items: center;
    justify-content: center;

    && svg {
        fill: url(#ask-datahub-icon-gradient);
        width: 20px;
        height: 20px;
    }
`;

/**
 * Ask DataHub icon with gradient fill matching the nav bar primary colors.
 *
 * Uses an inline hidden SVG to define the gradient because:
 * - Keeps gradient definition co-located with the component that uses it
 * - Avoids polluting the global SVG namespace
 * - Ensures gradient is available when component renders
 * - Makes the component self-contained and reusable
 */
export const AskDataHubIcon: React.FC = () => {
    return (
        <>
            {/* Hidden SVG for gradient definition - required for the fill to reference via url(#ask-datahub-icon-gradient) */}
            <svg
                style={{ width: 0, height: 0, position: 'absolute', visibility: 'hidden' }}
                aria-hidden="true"
                focusable="false"
            >
                <linearGradient id="ask-datahub-icon-gradient" x2="1" y2="1">
                    <stop offset="1%" stopColor={getColor('primary', 300)} />
                    <stop offset="99%" stopColor={getColor('primary', 500)} />
                </linearGradient>
            </svg>

            <IconWrapper>
                <Sparkle size={20} weight="fill" />
            </IconWrapper>
        </>
    );
};
