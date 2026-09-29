import React from 'react';
import styled from 'styled-components';

const ErrorContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 480px;
    background-color: ${(props) => props.theme.colors.bg};
    border: 2px dashed ${(props) => props.theme.colors.border};
    border-radius: 8px;
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 16px;
    text-align: center;
    padding: 20px;
`;

export const ErrorComponent = ({ message }: { message: string }) => {
    return <ErrorContainer>{message}</ErrorContainer>;
};
