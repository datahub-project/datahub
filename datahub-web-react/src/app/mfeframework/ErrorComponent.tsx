import React from 'react';
import styled from 'styled-components';

const ErrorContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 480px;
    background-color: #f5f5f5;
    border: 2px dashed #d9d9d9;
    border-radius: 8px;
    color: #595959;
    font-size: 16px;
    text-align: center;
    padding: 20px;
`;

export const ErrorComponent = ({ message }: { message: string }) => {
    return <ErrorContainer>{message}</ErrorContainer>;
};
