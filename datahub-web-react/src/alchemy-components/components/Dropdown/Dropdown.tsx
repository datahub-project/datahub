// Reset antd styles for the menu dropdown including submenu
// Styles can't be modified by styled as submenu is rendering in another portal
import '@components/components/Dropdown/reset-dropdown-menu-styles.less';

import { Dropdown as AntdDropdown } from 'antd';
import React, { useMemo } from 'react';

import { RESET_DROPDOWN_MENU_STYLES_CLASSNAME } from '@components/components/Dropdown/constants';
import { DropdownProps } from '@components/components/Dropdown/types';
import { useOverlayClassStackContext } from '@components/components/Utils/OverlayClassContext/OverlayClassContext';

export default function Dropdown({
    children,
    overlayClassName,
    resetDefaultMenuStyles,
    onOpenChange,
    ...props
}: React.PropsWithChildren<DropdownProps>) {
    // Get all overlay classes from parents
    const overlayClassNames = useOverlayClassStackContext();

    const finalOverlayClassName = useMemo(() => {
        const overlayClassNamesWithDefault = [
            ...overlayClassNames,
            ...(resetDefaultMenuStyles ? [RESET_DROPDOWN_MENU_STYLES_CLASSNAME] : []),
        ];

        if (overlayClassName) {
            return [...overlayClassNamesWithDefault, overlayClassName].join(' ');
        }
        return overlayClassNamesWithDefault.join(' ');
    }, [overlayClassName, overlayClassNames, resetDefaultMenuStyles]);

    // antd 4 closed only its internal state on a menu click, so controlled dropdowns stayed open unless an
    // item closed them. antd 5 asks the owner to close (source 'menu'); ignore that to keep antd 4 behavior.
    const handleOpenChange: DropdownProps['onOpenChange'] = (open, info) => {
        if (!open && info?.source === 'menu') return;
        onOpenChange?.(open, info);
    };

    return (
        <AntdDropdown
            trigger={['click']}
            {...props}
            onOpenChange={handleOpenChange}
            overlayClassName={finalOverlayClassName}
        >
            {children}
        </AntdDropdown>
    );
}
