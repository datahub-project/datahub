import React from 'react';

export interface DropdownProps {
    open?: boolean;
    overlayClassName?: string;
    disabled?: boolean;
    dropdownRender?: (menus: React.ReactNode) => React.ReactNode;
    onOpenChange?: (open: boolean) => void;
}
