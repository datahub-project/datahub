/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
/* eslint-disable import/no-cycle */
import { Dropdown } from 'antd';
import React, { useState } from 'react';

import { FilterField, FilterValue, FilterValueOption } from '@app/searchV2/filters/types';
import ValueMenu from '@app/searchV2/filters/value/ValueMenu';
import { EntityType, FacetFilterInput } from '@src/types.generated';

interface Props {
    field: FilterField;
    values: FilterValue[];
    defaultOptions: FilterValueOption[];
    onChangeValues: (newValues: FilterValue[]) => void;
    children?: any;
    className?: string;
    manuallyUpdateFilters?: (newValues: FacetFilterInput[]) => void;
    aggregationsEntityTypes?: Array<EntityType>;
}

export default function ValueSelector({
    field,
    values,
    defaultOptions,
    onChangeValues,
    children,
    className,
    manuallyUpdateFilters,
    aggregationsEntityTypes,
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
                    aggregationsEntityTypes={aggregationsEntityTypes}
                />
            )}
        >
            {children}
        </Dropdown>
    );
}
