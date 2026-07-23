import React from 'react';
import styled from 'styled-components';

const ActionsContainer = styled.div`
    display: flex;
    padding: 4px;
    justify-content: center;
    align-items: center;
    gap: 8px;
    width: fit-content;
    align-self: center;
    border-radius: 12px;
    box-shadow: ${(props) => props.theme.colors.shadowMd};

    background-color: ${(props) => props.theme.colors.bg};
    position: absolute;
    left: 50%;
    bottom: 2px;
    transform: translateX(-55%);
`;

export type ActionsBarProps = { children?: React.ReactNode; dataTestId?: string };

export const ActionsBar = ({ children, dataTestId }: ActionsBarProps) => {
    return <ActionsContainer data-testid={dataTestId}>{children}</ActionsContainer>;
};
