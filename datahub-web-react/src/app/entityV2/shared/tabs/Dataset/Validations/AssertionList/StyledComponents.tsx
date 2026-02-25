import styled from 'styled-components';

export const StyledTableContainer = styled.div`
    table tr.acryl-selected-table-row {
        background-color: ${(props) => props.theme.colors.bgSurface};
    }
    margin: 0px 12px 12px 12px;
`;
