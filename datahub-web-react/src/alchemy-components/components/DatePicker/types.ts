/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { DatePicker as AntdDatePicker } from 'antd';
import { Moment } from 'moment';

import { DatePickerVariant } from '@components/components/DatePicker/constants';

export type DatePickerProps = {
    value?: DatePickerValue;
    onChange?: (value: DatePickerValue) => void;
    disabled?: boolean;
    disabledDate?: (value: DatePickerValue) => boolean;
    variant?: DatePickerVariant;
    placeholder?: string;
};

export type DatePickerState = {
    open?: boolean;
    value?: DatePickerValue;
    setValue?: React.Dispatch<React.SetStateAction<DatePickerValue>>;
};

export type ExtendedInputRenderProps = React.InputHTMLAttributes<HTMLInputElement> & {
    datePickerProps: DatePickerProps;
    datePickerState: DatePickerState;
};

export type AntdDatePickerProps = React.ComponentProps<typeof AntdDatePicker>;

export type DatePickerValue = Moment | null | undefined;

export type VariantProps = Omit<AntdDatePickerProps, 'inputRender'> & {
    inputRender?: (props: ExtendedInputRenderProps) => React.ReactNode;
    $noDefaultPaddings?: boolean;
};
