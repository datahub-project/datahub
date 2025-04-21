import { DatePicker as AntdDatePicker } from 'antd';
import styled from 'styled-components';

import { getColor } from '@src/alchemy-components/theme/utils';

export const StyledAntdDatePicker = styled(AntdDatePicker)<{ $noDefaultPaddings?: boolean }>`
    &.ant-picker {
        ${(props) => props.$noDefaultPaddings && 'padding: 0;'}
        width: 100%;
    }

    &.acryl-date-picker .ant-picker-cell-today > .ant-picker-cell-inner::before {
        border: 1px solid ${({ theme }) => getColor('primary', 500, theme)} !important;
    }
`;

export const StyledCalendarWrapper = styled.div`
    & .ant-picker-cell-selected > .ant-picker-cell-inner {
        background: ${({ theme }) => getColor('primary', 500, theme)} !important;
    }

    & .ant-picker-cell-today > .ant-picker-cell-inner::before {
        border: 1px solid ${({ theme }) => getColor('primary', 500, theme)} !important;
    }

    & .ant-picker-today-btn {
        color: ${({ theme }) => getColor('primary', 500, theme)};
    }

    & .ant-picker-header-view button:hover {
        color: ${({ theme }) => getColor('primary', 500, theme)};
    }
`;
