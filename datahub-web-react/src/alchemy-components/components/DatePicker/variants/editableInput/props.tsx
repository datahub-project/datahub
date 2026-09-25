import React from 'react';

import { VariantProps } from '@components/components/DatePicker/types';
import { DefaultDatePickerInput } from '@components/components/DatePicker/variants/common/components';
import { CommonVariantProps } from '@components/components/DatePicker/variants/common/props';

export const EditableInputVariantProps: VariantProps = {
    ...CommonVariantProps,
    // Only the first entry drives display; the rest are parse-only fallbacks so pasted dates are
    // accepted. Keep them unambiguous — dayjs parses non-strictly and takes the first format that
    // yields a valid date, so e.g. MM/DD and DD/MM together would silently misread 12/01/2026.
    format: ['ll', 'YYYY-MM-DD', 'YYYY/MM/DD'],
    inputRender: (props) => <DefaultDatePickerInput {...props} />,
};
