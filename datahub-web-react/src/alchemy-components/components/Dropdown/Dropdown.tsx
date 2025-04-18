import { Dropdown as AntdDropdown } from 'antd';
import React, { useMemo } from 'react';
<<<<<<< HEAD

import { DropdownProps } from '@components/components/Dropdown/types';
import { useOverlayClassStackContext } from '@components/components/Utils/OverlayClassContext/OverlayClassContext';
=======
import { useOverlayClassStackContext } from '../Utils/OverlayClassContext/OverlayClassContext';
import { DropdownProps } from './types';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

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
