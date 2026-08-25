import React from 'react';

import { VariantProps } from '@components/components/DatePicker/types';
import { DefaultDatePickerInput } from '@components/components/DatePicker/variants/common/components';
import { CommonVariantProps } from '@components/components/DatePicker/variants/common/props';

export const EditableInputVariantProps: VariantProps = {
    ...CommonVariantProps,
    inputRender: (props) => <DefaultDatePickerInput {...props} />,
};
