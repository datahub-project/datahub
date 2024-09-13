/* eslint-disable import/no-cycle */
import { Dropdown } from 'antd';
import React, { useState } from 'react';
import { FilterField, FilterValue, FilterValueOption } from '../types';
import ValueMenu from './ValueMenu';

interface Props {
    field: FilterField;
    values: FilterValue[];
    defaultOptions: FilterValueOption[];
    onChangeValues: (newValues: FilterValue[]) => void;
    children?: any;
    className?: string;
}

export default function ValueSelector({ field, values, defaultOptions, onChangeValues, children, className }: Props) {
    const [isMenuOpen, setIsMenuOpen] = useState(false);

    const onUpdateValues = (newValues: FilterValue[]) => {
        setIsMenuOpen(false);
        onChangeValues(newValues);
    };

    return (
        <Dropdown
            key={field.field}
            trigger={['click']}
            open={isMenuOpen}
            onOpenChange={setIsMenuOpen}
            dropdownRender={(_) => (
                <ValueMenu
                    field={field}
                    values={values}
                    defaultOptions={defaultOptions}
                    onChangeValues={onUpdateValues}
                    visible={isMenuOpen}
                    includeCount
                    className={className}
                />
            )}
        >
            {children}
        </Dropdown>
    );
}
