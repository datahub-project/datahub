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
    box-shadow: 0px 4px 12px 0px rgba(9, 1, 61, 0.12);

    background-color: white;
    position: absolute;
    left: 50%;
    bottom: 2px;
    transform: translateX(-55%);
`;

export type ActionsBarProps = { children?: React.ReactNode };

export const ActionsBar = ({ children }: ActionsBarProps) => {
    return <ActionsContainer>{children}</ActionsContainer>;
};
