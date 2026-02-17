import styled from 'styled-components';

export const Card = styled.div`
    background-color: ${(props) => props.theme.colors.bg};
    border: 2px solid ${(props) => props.theme.colors.border};
    border-radius: 8px;
    cursor: pointer;
    display: flex;
    align-items: center;

    :hover {
        border: 2px solid ${(props) => props.theme.colors.textInformation};
        cursor: pointer;
    }
`;
