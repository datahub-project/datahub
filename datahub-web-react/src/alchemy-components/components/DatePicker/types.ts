import { DatePicker as AntdDatePicker } from 'antd';
import { Moment } from 'moment';
import { DatePickerVariant } from './constants';

export type DatePickerProps = {
    value?: DatePickerValue;
    onChange?: (value: DatePickerValue) => void;
    disabled?: boolean;
    disabledDate?: (value: DatePickerValue) => boolean;
    variant?: DatePickerVariant;
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
