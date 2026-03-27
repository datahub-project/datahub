import styled from 'styled-components';

export const IconLabelContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

export const ImageContainer = styled.div`
    display: flex;
    align-items: center;
    margin-right: 0px;
`;

export const Label = styled.span`
    font-family: Mulish;
    font-size: 14px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.text};
    white-space: normal;
`;
