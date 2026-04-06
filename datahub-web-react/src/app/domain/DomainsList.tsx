import styled from 'styled-components';

export const DomainsPaginationContainer = styled.div`
    display: flex;
    justify-content: center;
    padding: 12px;
    padding-left: 16px;
    border-bottom: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};
    display: flex;
    justify-content: space-between;
    align-items: center;
`;
