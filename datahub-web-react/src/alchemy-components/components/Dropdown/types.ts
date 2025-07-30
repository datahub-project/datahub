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
};
