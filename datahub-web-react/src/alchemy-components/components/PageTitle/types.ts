import React from 'react';

import { ButtonProps } from '@components/components/Button/types';

export interface PageTitleProps {
    title: string;
    subTitle?: string | React.ReactNode;
    pillLabel?: string;
    variant?: 'pageHeader' | 'sectionHeader';
    actionButton?: ButtonProps & { label: string };
    titlePill?: React.ReactNode;
}
