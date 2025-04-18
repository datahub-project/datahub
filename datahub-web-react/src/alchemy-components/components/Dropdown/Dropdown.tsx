import { Dropdown as AntdDropdown } from 'antd';
import React, { useMemo } from 'react';
import { useOverlayClassStackContext } from '../Utils/OverlayClassContext/OverlayClassContext';
import { DropdownProps } from './types';

export default function Dropdown({ children, overlayClassName, ...props }: React.PropsWithChildren<DropdownProps>) {
    // Get all overlay classes from parents
    const overlayClassNames = useOverlayClassStackContext();
    const finalOverlayClassName = useMemo(() => {
        if (overlayClassName) {
            return [...overlayClassNames, overlayClassName].join(' ');
        }
        return overlayClassNames.join(' ');
    }, [overlayClassName, overlayClassNames]);

    return (
        <AntdDropdown trigger={['click']} {...props} overlayClassName={finalOverlayClassName}>
            {children}
        </AntdDropdown>
    );
}
