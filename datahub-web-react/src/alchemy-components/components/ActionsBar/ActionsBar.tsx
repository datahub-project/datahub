import React from 'react';
import styled from 'styled-components';

const ActionsContainer = styled.div<{ $fullWidth?: boolean }>`
    display: flex;
    align-items: center;
    gap: 8px;
    border-radius: 12px;
    box-shadow: ${(props) => props.theme.colors.shadowMd};

    background-color: ${(props) => props.theme.colors.bg};
    position: absolute;
    bottom: 2px;

    ${({ $fullWidth }) =>
        $fullWidth
            ? `
                /* Flush to the container edges with no radius of its own, so the page card's
                   own border-radius + overflow does the clipping and the bar reads as part of it. */
                left: 0;
                right: 0;
                bottom: 0;
                border-radius: 0;
                padding: 12px 20px;
                justify-content: space-between;
              `
            : `
                left: 50%;
                padding: 4px;
                justify-content: center;
                width: fit-content;
                align-self: center;
                transform: translateX(-55%);
              `}
`;

export type ActionsBarProps = {
    children?: React.ReactNode;
    dataTestId?: string;
    /** Stretch the bar across its container and space children apart, instead of a centered pill. */
    fullWidth?: boolean;
};

export const ActionsBar = ({ children, dataTestId, fullWidth }: ActionsBarProps) => {
    return (
        <ActionsContainer data-testid={dataTestId} $fullWidth={fullWidth}>
            {children}
        </ActionsContainer>
    );
};
