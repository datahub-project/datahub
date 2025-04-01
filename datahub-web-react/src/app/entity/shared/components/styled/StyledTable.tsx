import React from 'react';
import { Table } from 'antd';
import styled from 'styled-components';
import { colors, typography } from '@src/alchemy-components/theme';

const commonTableStyles = `
    width: 100%;
`;

const commonCellStyles = `
    background-color: #fff;
    white-space: nowrap;
`;

const commonHeaderStyles = `
    background-color: ${colors.gray[1500]};
    font-size: 12px;
    font-weight: 700;
    color: ${colors.gray[1700]};
    border-bottom: 1px solid ${colors.gray[1400]};
    border-right: 1px solid ${colors.gray[1400]};
    padding: 0 16px;
    height: 42px;
    position: sticky;
    top: 0;
`;

const commonBodyStyles = `
    color: ${colors.gray[600]};
    font-size: ${typography.fontSizes.md};
    font-weight: ${typography.fontWeights.normal};
    padding: 16px 8px 16px 16px;
`;

const commonScrollStyles = `
    min-height: 100px;  /* Ensure there's always some height for Cypress */
`;

const commonPaginationStyles = `
    margin: 0;
    padding: 12px 16px;
    background: #fff;
    border-top: none;
`;

const commonFirstColumnStyles = `
    font-weight: ${typography.fontWeights.bold};
    color: ${colors.gray[600]};
    font-size: 12px;
`;

const commonLastColumnStyles = `
    text-align: right;
    padding-right: 16px;
`;

const TableWrapper = styled.div`
    margin-top: 16px;
    margin-left: 20px;
    margin-right: 20px;
    width: calc(100% - 40px);
    border: 1px solid ${colors.gray[1400]};
    border-radius: 12px;
    overflow: hidden;
`;

const BaseStyledTable = styled(Table)`
    overflow: inherit;
    height: inherit;

    /* Container styles */
    &&& .ant-table {
        ${commonTableStyles}
    }

    &&& .ant-table-container {
        width: 100%;
        overflow: inherit;
    }

    &&& .ant-table-content {
        width: 100%;
        overflow: inherit;
    }

    &&& .ant-table-body {
        ${commonScrollStyles}
        overflow: inherit;
    }

    &&& .ant-table-cell {
        ${commonCellStyles}
    }

    &&& .ant-table-thead .ant-table-cell {
        ${commonHeaderStyles}
    }

    &&& .ant-table-thead .ant-table-cell:last-child {
        ${commonLastColumnStyles}
        border-right: none;
    }

    &&& .ant-table-thead .ant-table-cell::before {
        display: none !important;
    }

    &&& .ant-table-thead .ant-table-cell:first-child {
        border-top-left-radius: 12px;
    }

    &&& .ant-table-thead .ant-table-cell:last-child {
        border-top-right-radius: 12px;
    }

    &&& .ant-table-tbody td,
    &&& .ant-table-tbody .ant-table-cell {
        ${commonBodyStyles}
    }

    &&& .ant-table-tbody > tr > td:last-child {
        ${commonLastColumnStyles}
    }

    &&& .ant-table-tbody > tr > td:first-child {
        ${commonFirstColumnStyles}
    }

    &&& tr {
        height: 32px;
    }

    &&& .ant-table-tbody > tr > td {
        border-bottom: 1px solid ${colors.gray[1400]};
    }

    &&& .ant-table-tbody > tr:last-child > td {
        border-bottom: none;
    }

    /* Expanded table styles */
    &&& .ant-table-expanded-row {
        &&& .ant-table-cell {
            ${commonCellStyles}
        }

        &&& .ant-table-thead .ant-table-cell {
            ${commonHeaderStyles}
        }

        &&& .ant-table-tbody td,
        &&& .ant-table-tbody .ant-table-cell {
            ${commonBodyStyles}
        }

        &&& .ant-table-tbody > tr > td:first-child {
            ${commonFirstColumnStyles}
        }
    }

    /* Pagination styles */
    &&& .ant-table-pagination,
    &&& .ant-table-expanded-row .ant-table-pagination {
        ${commonPaginationStyles}
    }

    &&& .ant-pagination,
    &&& .ant-table-expanded-row .ant-pagination {
        border-top: none;
    }

    &&& .ant-table-pagination-holder {
        border-top: none;
    }

    &&& span {
        border-top: none !important;
    }
` as typeof Table;

export const StyledTable = (props: any) => (
    <TableWrapper data-testid="styled-table-wrapper">
        <BaseStyledTable {...props} />
    </TableWrapper>
);
