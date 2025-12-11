/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { DatePicker as AntdDatePicker } from 'antd';
import styled from 'styled-components';

export const StyledAntdDatePicker = styled(AntdDatePicker)<{ $noDefaultPaddings?: boolean }>`
    &.ant-picker {
        ${(props) => props.$noDefaultPaddings && 'padding: 0;'}
        width: 100%;
    }

    &.acryl-date-picker .ant-picker-cell-today > .ant-picker-cell-inner::before {
        border: 1px solid ${({ theme }) => theme.styles['primary-color']} !important;
    }
`;

export const StyledCalendarWrapper = styled.div`
    & .ant-picker-cell-selected > .ant-picker-cell-inner {
        background: ${({ theme }) => theme.styles['primary-color']} !important;
    }

    & .ant-picker-cell-today > .ant-picker-cell-inner::before {
        border: 1px solid ${({ theme }) => theme.styles['primary-color']} !important;
    }

    & .ant-picker-today-btn {
        color: ${({ theme }) => theme.styles['primary-color']};
    }

    & .ant-picker-header-view button:hover {
        color: ${({ theme }) => theme.styles['primary-color']};
    }
`;
