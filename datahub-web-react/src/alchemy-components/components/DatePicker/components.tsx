import styled from 'styled-components';

import { formLabelTextStyles } from '@components/components/commonStyles';
import { spacing } from '@components/theme';

import AntdDatePicker from '@utils/DayjsDatePicker';

export const DatePickerWrapper = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
`;

export const Label = styled.div(({ theme }) => ({
    ...formLabelTextStyles,
    color: theme.colors.text,
    marginBottom: spacing.xxsm,
    textAlign: 'left',
}));

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
