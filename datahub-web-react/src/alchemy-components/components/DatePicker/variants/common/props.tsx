import React from 'react';

import { StyledCalendarWrapper } from '@components/components/DatePicker/components';
import { VariantProps } from '@components/components/DatePicker/types';

export const CommonVariantProps: VariantProps = {
    panelRender: (panel) => <StyledCalendarWrapper>{panel}</StyledCalendarWrapper>,
};
