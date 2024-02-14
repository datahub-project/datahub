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
    alignRight?: boolean;
}

export default function ValueSelector({ field, values, defaultOptions, onChangeValues, children, alignRight }: Props) {
    const [isMenuOpen, setIsMenuOpen] = useState(false);

    const onUpdateValues = (newValues: FilterValue[]) => {
        setIsMenuOpen(false);
        onChangeValues(newValues);
    };

    return (
        <Dropdown
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
                    alignRight={alignRight}
                />
            )}
        >
            {children}
        </Dropdown>
    );
}
