import React from 'react';
import { DefaultOptionType } from 'antd/lib/select';

export type ValueType = string;
export type OptionType = DefaultOptionType;
export interface AutoCompleteProps {
    dataTestId?: string;
    className?: string;

    value?: ValueType;
    defaultValue?: ValueType;
    options: OptionType[];
    open?: boolean;

    defaultActiveFirstOption?: boolean;
    filterOption?: boolean | ((inputValue: ValueType, option?: OptionType) => boolean);
    dropdownContentHeight?: number;

    onSelect?: (value: ValueType, option: OptionType) => void;
    onSearch?: (value: ValueType) => void;
    onChange?: (value: ValueType, option: OptionType | OptionType[]) => void;
    onDropdownVisibleChange?: (isOpen: boolean) => void;

    dropdownRender?: (menu: React.ReactElement) => React.ReactElement | undefined;

    style?: React.CSSProperties;
    dropdownStyle?: React.CSSProperties;

    showWrapping?: boolean;
}
