import styled from 'styled-components';

export const IconWrapper = styled.div<{ size: string; rotate?: string }>`
    position: relative;

    display: flex;
    align-items: center;
    justify-content: center;

    width: ${({ size }) => size};
    height: ${({ size }) => size};

    & svg {
        width: 100%;
        height: 100%;

        transform: ${({ rotate }) => rotate};
    }
`;
