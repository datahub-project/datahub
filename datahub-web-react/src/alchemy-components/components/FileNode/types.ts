import React from 'react';

import { FontSizeOptions } from '@components/theme/config';

export interface FileNodeProps {
    fileName?: string;
    loading?: boolean;
    border?: boolean;
    extraRightContent?: React.ReactNode;
    className?: string;
    size?: FontSizeOptions;
    onClick?: (e: React.MouseEvent) => void;
    onClose?: (e: React.MouseEvent) => void;
}
