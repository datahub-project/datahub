/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { DropdownProps as AntdDropdownProps } from 'antd';

export type DropdownProps = Pick<
    AntdDropdownProps,
    | 'open'
    | 'overlayClassName'
    | 'disabled'
    | 'dropdownRender'
    | 'onOpenChange'
    | 'placement'
    | 'menu'
    | 'trigger'
    | 'destroyPopupOnHide'
    | 'overlayStyle'
> & {
    resetDefaultMenuStyles?: boolean;
    dataTestId?: string;
};
