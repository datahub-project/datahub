import styled from 'styled-components';

export const StyledImage = styled.img`
    cursor: pointer;
`;

export const Label = styled.span`
    font-family: Mulish;
    font-size: 14px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.text};
    white-space: normal;
`;
