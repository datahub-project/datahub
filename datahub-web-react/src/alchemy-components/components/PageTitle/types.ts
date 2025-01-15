import React from 'react';

export interface PageTitleProps {
    title: string;
    subTitle?: string | React.ReactNode;
    pillLabel?: string;
    variant?: 'pageHeader' | 'sectionHeader';
}
