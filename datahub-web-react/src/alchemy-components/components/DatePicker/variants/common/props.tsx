import React from 'react';
import { StyledCalendarWrapper } from '../../components';
import { VariantProps } from '../../types';

export const CommonVariantProps: VariantProps = {
    panelRender: (panel) => <StyledCalendarWrapper>{panel}</StyledCalendarWrapper>,
};
