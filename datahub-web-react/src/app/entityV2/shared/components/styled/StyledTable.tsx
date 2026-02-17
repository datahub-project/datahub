import { Table } from 'antd';
import styled from 'styled-components';

export const StyledTable = styled(Table)`
    overflow: inherit;
    height: inherit;

    &&& .ant-table-cell {
        background-color: ${(props) => props.theme.colors.bgSurface};
    }
    &&& .ant-table-thead .ant-table-cell {
        font-weight: 700;
        font-size: 12px;
        color: ${(props) => props.theme.colors.text};
        background-color: ${(props) => props.theme.colors.bgSurface};
    }
    &&& .ant-table-thead > tr > th {
        padding-left: 10px;
    }

    &&
        .ant-table-thead
        > tr
        > th:not(:last-child):not(.ant-table-selection-column):not(.ant-table-row-expand-icon-cell):not(
            [colspan]
        )::before {
        border: 1px solid ${(props) => props.theme.colors.bgSurface};
    }

    &&& tr {
        height: 32px;
    }

    &&& td {
        background-color: inherit;
    }
` as typeof Table;
// this above line preserves the Table component's generic-ness

export const CompactStyledTable = styled(Table)`
    overflow: inherit;
    height: inherit;

    &&& .ant-table-cell {
        background-color: ${(props) => props.theme.colors.bgSurface};
    }
    &&& .ant-table-thead .ant-table-cell {
        font-weight: 600;
        font-size: 12px;
        color: ${(props) => props.theme.colors.textSecondary};
    }
    &&
        .ant-table-thead
        > tr
        > th:not(:last-child):not(.ant-table-selection-column):not(.ant-table-row-expand-icon-cell):not(
            [colspan]
        )::before {
        border: 1px solid ${(props) => props.theme.colors.bgSurface};
    }

    &&& td {
        background-color: inherit;
    }
` as typeof Table;
