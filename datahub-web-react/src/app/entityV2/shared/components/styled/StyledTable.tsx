import { Table } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../constants';

export const StyledTable = styled(Table)`
    overflow: inherit;
    height: inherit;

    &&& .ant-table-cell {
        background-color: #fff;
    }
    &&& .ant-table-thead .ant-table-cell {
        font-weight: 700;
        font-size: 12px;
        color: ${REDESIGN_COLORS.HEADING_COLOR};
        background-color: ${REDESIGN_COLORS.LIGHT_GREY};
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
        border: 1px solid ${ANTD_GRAY[4]};
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
        background-color: #fff;
    }
    &&& .ant-table-thead .ant-table-cell {
        font-weight: 600;
        font-size: 12px;
        color: ${ANTD_GRAY[8]};
    }
    &&
        .ant-table-thead
        > tr
        > th:not(:last-child):not(.ant-table-selection-column):not(.ant-table-row-expand-icon-cell):not(
            [colspan]
        )::before {
        border: 1px solid ${ANTD_GRAY[4]};
    }

    &&& td {
        background-color: inherit;
    }
` as typeof Table;
