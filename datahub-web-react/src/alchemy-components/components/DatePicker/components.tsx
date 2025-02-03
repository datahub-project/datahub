import { colors } from '@src/alchemy-components/theme';
import { DatePicker as AntdDatePicker } from 'antd';
import styled from 'styled-components';

export const StyledAntdDatePicker = styled(AntdDatePicker)<{ $noDefaultPaddings?: boolean }>`
    &.ant-picker {
        ${(props) => props.$noDefaultPaddings && 'padding: 0;'}
        width: 100%;
    }

    &.acryl-date-picker .ant-picker-cell-today > .ant-picker-cell-inner::before {
        border: 1px solid ${colors.violet[500]} !important;
    }
`;

export const StyledCalendarWrapper = styled.div`
    & .ant-picker-cell-selected > .ant-picker-cell-inner {
        background: ${colors.violet[500]} !important;
    }

    & .ant-picker-cell-today > .ant-picker-cell-inner::before {
        border: 1px solid ${colors.violet[500]} !important;
    }

    & .ant-picker-today-btn {
        color: ${colors.violet[500]};
    }

    & .ant-picker-header-view button:hover {
        color: ${colors.violet[500]};
    }
`;
