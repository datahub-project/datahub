import { DropdownProps as AntdDropdwonProps } from 'antd';

export type DropdownProps = Pick<
    AntdDropdwonProps,
    'open' | 'overlayClassName' | 'disabled' | 'dropdownRender' | 'onOpenChange' | 'placement'
>;
