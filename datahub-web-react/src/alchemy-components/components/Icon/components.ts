import styled from 'styled-components';

export const IconWrapper = styled.span<{ rotate?: string }>`
    display: inline-flex;
    align-items: center;
    justify-content: center;

    & svg {
        transform: ${({ rotate }) => rotate};
    }
`;
