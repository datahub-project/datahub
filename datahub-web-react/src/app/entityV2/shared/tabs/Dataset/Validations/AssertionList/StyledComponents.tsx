import { ANTD_GRAY } from '@src/app/entityV2/shared/constants';
import { Table } from 'antd';
import styled from 'styled-components';

export const AssertionListStyledTable = styled(Table)`
    max-width: none;
    &&& .ant-table-thead .ant-table-cell {
        font-weight: 600;
        font-size: 12px;
        color: ${ANTD_GRAY[8]};
    }
    &&& .ant-table-expanded-row > .ant-table-cell {
        padding: 0px;
    }
    &&& .ant-table-tbody > tr > td > .ant-table-wrapper:only-child .ant-table,
    .ant-table-tbody > tr > td > .ant-table-expanded-row-fixed > .ant-table-wrapper:only-child .ant-table {
        margin-left: 0px;
    }
    &&
        .ant-table-thead
        > tr
        > th:not(:last-child):not(.ant-table-selection-column):not(.ant-table-row-expand-icon-cell):not(
            [colspan]
        )::before {
        border: 1px solid ${ANTD_GRAY[4]};
    }
    &&& .ant-table-thead > tr > th {
        line-height: 5px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    &&& .ant-table-cell {
        background-color: transparent;
    }

    &&& .acryl-selected-assertions-table-row {
        background-color: ${ANTD_GRAY[4]};
    }

    .group-header {
        cursor: pointer;
        background-color: ${ANTD_GRAY[3]};
    }
    &&& .acryl-assertions-table-row {
        cursor: pointer;
        background-color: ${ANTD_GRAY[2]};
        :hover {
            background-color: ${ANTD_GRAY[3]};
        }
    }
`;
