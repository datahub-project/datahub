import { Select, Tag } from 'antd';
import React from 'react';

import { SelectOption } from '@app/sharedV2/queryBuilder/builder/property/types/values';

type Props = {
    options: SelectOption[];
    selected?: string[];
    mode?: 'multiple' | 'tags';
    placeholder?: string;
    style?: any;
    tagStyle?: React.CSSProperties;
    optionStyle?: React.CSSProperties;
    onChangeSelected: (newSelectedIds: string[] | undefined) => void;
};

export const SelectInput = ({
    options,
    selected,
    mode,
    placeholder,
    style,
    tagStyle,
    optionStyle,
    onChangeSelected,
}: Props) => {
    const onSelect = (id) => {
        const newSelected = [...(selected || []), id];
        onChangeSelected(newSelected);
    };

    const onDeselect = (id) => {
        if (Array.isArray(selected)) {
            onChangeSelected(selected.filter((item) => item !== id));
        } else {
            onChangeSelected(undefined);
        }
    };

    return (
        <Select
            style={style}
            value={selected}
            mode={mode}
            placeholder={placeholder || 'Select values...'}
            onSelect={onSelect}
            onDeselect={onDeselect}
            tagRender={(tagProps) => (
                <Tag closable={tagProps.closable} onClose={tagProps.onClose} style={tagStyle}>
                    {tagProps.label}
                </Tag>
            )}
        >
            {options.map((option) => (
                <Select.Option value={option.id} style={optionStyle}>
                    <span> {option.displayName}</span>
                </Select.Option>
            ))}
        </Select>
    );
};
