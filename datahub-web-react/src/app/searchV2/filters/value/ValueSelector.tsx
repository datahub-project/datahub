/* eslint-disable import/no-cycle */
import { Dropdown } from 'antd';
import React, { useState } from 'react';

import { FilterField, FilterValue, FilterValueOption } from '@app/searchV2/filters/types';
import ValueMenu from '@app/searchV2/filters/value/ValueMenu';
import { FacetFilterInput } from '@src/types.generated';

interface Props {
    field: FilterField;
    values: FilterValue[];
    defaultOptions: FilterValueOption[];
    onChangeValues: (newValues: FilterValue[]) => void;
    children?: any;
    className?: string;
    manuallyUpdateFilters?: (newValues: FacetFilterInput[]) => void;
}

export default function ValueSelector({
    field,
    values,
    defaultOptions,
    onChangeValues,
    children,
    className,
    manuallyUpdateFilters,
}: Props) {
    const [isMenuOpen, setIsMenuOpen] = useState(false);

    const onUpdateValues = (newValues: FilterValue[]) => {
        setIsMenuOpen(false);
        onChangeValues(newValues);
    };

    const onManuallyUpdateFilters = (newValues: FacetFilterInput[]) => {
        setIsMenuOpen(false);
        manuallyUpdateFilters?.(newValues);
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
                    manuallyUpdateFilters={onManuallyUpdateFilters}
                />
            )}
        >
            {children}
        </Dropdown>
    );
}
