import { DatePicker as AntdDatePicker } from 'antd';
import styled from 'styled-components';

export const StyledAntdDatePicker = styled(AntdDatePicker)<{ $noDefaultPaddings?: boolean }>`
    &.ant-picker {
        ${(props) => props.$noDefaultPaddings && 'padding: 0;'}
        width: 100%;
    }

    &.acryl-date-picker .ant-picker-cell-today > .ant-picker-cell-inner::before {
        border: 1px solid ${({ theme }) => theme.colors.borderBrand} !important;
    }
`;

export const StyledCalendarWrapper = styled.div`
    & .ant-picker-cell-selected > .ant-picker-cell-inner {
        background: ${({ theme }) => theme.colors.buttonFillBrand} !important;
    }

    & .ant-picker-cell-today > .ant-picker-cell-inner::before {
        border: 1px solid ${({ theme }) => theme.colors.borderBrand} !important;
    }

    & .ant-picker-today-btn {
        color: ${({ theme }) => theme.colors.textBrand};
    }

    & .ant-picker-header-view button:hover {
        color: ${({ theme }) => theme.colors.textHover};
    }
`;
