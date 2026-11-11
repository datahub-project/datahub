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

    // antd 5 also asks to close when a Menu inside custom dropdownRender content is clicked (source
    // 'menu'); antd 4 only did that for the menu prop. Keep custom content (e.g. filter checkboxes) open.
    const handleOpenChange: DropdownProps['onOpenChange'] = (open, info) => {
        if (!open && info?.source === 'menu' && props.dropdownRender && !props.menu) return;
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
