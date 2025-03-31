import { Table } from 'antd';
import styled from 'styled-components';
import { colors, typography } from '@src/alchemy-components/theme';

const commonTableStyles = `
    border-radius: 12px;
    border: 1px solid ${colors.gray[1400]};
    overflow: hidden;
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
`;

const commonBodyStyles = `
    color: ${colors.gray[600]};
    font-size: ${typography.fontSizes.md};
    font-weight: ${typography.fontWeights.normal};
    padding: 16px 8px 16px 16px;
`;

const commonScrollStyles = `
    overflow-y: visible;
    overflow-x: visible;
    &::-webkit-scrollbar {
        display: none;
    }
    -ms-overflow-style: none;
    scrollbar-width: none;
    min-height: 100px;  /* Ensure there's always some height for Cypress */
`;

const commonBorderRadiusStyles = `
    border-radius: 12px;
    overflow: hidden;
`;

const commonPaginationStyles = `
    margin: 0;
    padding: 12px 16px;
    background: #fff;
    border-top: none;
`;

const commonSorterStyles = `
    font-size: 10px;
    transform: scale(0.8);
    color: ${colors.gray[1800]};
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

export const StyledTable = styled(Table)`
    margin: 16px 20px 0px 20px;

    /* Main table styles */
    &&& .ant-table {
        ${commonTableStyles}
    }

    &&& .ant-table-container,
    &&& .ant-table-content {
        ${commonBorderRadiusStyles}
    }

    &&& .ant-table-body {
        ${commonScrollStyles}
    }

    &&& .ant-table-scroll,
    &&& .ant-table-scrollbar {
        overflow: hidden;
        display: none;
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

    &&& .ant-table-column-title,
    &&& .ant-table-column-sorter {
        font-size: 12px;
        font-weight: 700;
        color: ${colors.gray[1700]};
    }

    &&& .ant-table-column-sorter-inner {
        ${commonSorterStyles}
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

    &&& .ant-table-tbody > tr:last-child > td:first-child {
        border-bottom-left-radius: 12px;
    }

    &&& .ant-table-tbody > tr:last-child > td:last-child {
        border-bottom-right-radius: 12px;
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
    &&& .ant-table-expanded-row .ant-table-wrapper {
        margin: 8px 0;
    }

    &&& .ant-table-expanded-row .ant-table {
        ${commonTableStyles}
    }

    &&& .ant-table-expanded-row .ant-table-container,
    &&& .ant-table-expanded-row .ant-table-content {
        ${commonBorderRadiusStyles}
    }

    &&& .ant-table-expanded-row .ant-table-body {
        ${commonScrollStyles}
    }

    &&& .ant-table-expanded-row .ant-table-scroll,
    &&& .ant-table-expanded-row .ant-table-scrollbar {
        overflow: hidden;
        display: none;
    }

    &&& .ant-table-expanded-row .ant-table-cell {
        ${commonCellStyles}
    }

    &&& .ant-table-expanded-row .ant-table-thead .ant-table-cell {
        ${commonHeaderStyles}
    }

    &&& .ant-table-expanded-row .ant-table-thead .ant-table-cell:last-child {
        ${commonLastColumnStyles}
        border-right: none;
    }

    &&& .ant-table-expanded-row .ant-table-thead .ant-table-cell::before {
        display: none !important;
    }

    &&& .ant-table-expanded-row .ant-table-thead .ant-table-cell:first-child {
        border-top-left-radius: 12px;
    }

    &&& .ant-table-expanded-row .ant-table-thead .ant-table-cell:last-child {
        border-top-right-radius: 12px;
    }

    &&& .ant-table-expanded-row .ant-table-column-title,
    &&& .ant-table-expanded-row .ant-table-column-sorter {
        font-size: 12px;
        font-weight: 700;
        color: ${colors.gray[1700]};
    }

    &&& .ant-table-expanded-row .ant-table-column-sorter-inner {
        ${commonSorterStyles}
    }

    &&& .ant-table-expanded-row .ant-table-tbody td,
    &&& .ant-table-expanded-row .ant-table-tbody .ant-table-cell {
        ${commonBodyStyles}
    }

    &&& .ant-table-expanded-row .ant-table-tbody > tr > td:first-child {
        ${commonFirstColumnStyles}
    }

    &&& .ant-table-expanded-row .ant-table-tbody > tr:last-child > td:first-child {
        border-bottom-left-radius: 12px;
    }

    &&& .ant-table-expanded-row .ant-table-tbody > tr:last-child > td:last-child {
        border-bottom-right-radius: 12px;
    }

    &&& .ant-table-expanded-row tr {
        height: 32px;
    }

    &&& .ant-table-expanded-row .ant-table-tbody > tr > td {
        border-bottom: 1px solid ${colors.gray[1400]};
    }

    &&& .ant-table-expanded-row .ant-table-tbody > tr:last-child > td {
        border-bottom: none;
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
// this above line preserves the Table component's generic-ness
