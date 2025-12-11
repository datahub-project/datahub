/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { DefaultOptionType } from 'antd/lib/select';
import { AlignType } from 'rc-trigger/lib/interface';
import React from 'react';

export type ValueType = string;

export type OptionType = DefaultOptionType;

export type AutocompleteDropdownAlign = AlignType;

export interface AutoCompleteProps {
    dataTestId?: string;
    id?: string;
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
    onClear?: () => void;
    onDropdownVisibleChange?: (isOpen: boolean) => void;

    dropdownRender?: (menu: React.ReactElement) => React.ReactElement | undefined;
    notFoundContent?: React.ReactNode;

    dropdownAlign?: AutocompleteDropdownAlign;
    style?: React.CSSProperties;
    dropdownStyle?: React.CSSProperties;
    dropdownMatchSelectWidth?: boolean | number;

    clickOutsideWidth?: string;
}
