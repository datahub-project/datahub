/* eslint-disable import/no-cycle */
import { Button, Icon, Menu } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useCallback, useMemo } from 'react';
import styled from 'styled-components';

import { ItemType } from '@components/components/Menu/types';

import { DEFAULT_FILTER_FIELDS } from '@app/searchV2/filters/field/fields';
import { FilterField, FilterPredicate } from '@app/searchV2/filters/types';
import ValueMenu from '@app/searchV2/filters/value/ValueMenu';
import { getDefaultFieldOperatorType } from '@app/searchV2/filters/value/utils';

const AddFilterButton = styled(Button)`
    margin: 0px;
    padding: 4px;
    width: fit-content;
`;

interface Props {
    fields?: FilterField[];
    onAddFilter: (predicate: FilterPredicate) => void;
    includeCount?: boolean;
}

export default function AddFilterDropdown({ fields = DEFAULT_FILTER_FIELDS, onAddFilter, includeCount }: Props) {
    const handleAddFilter = useCallback(
        (field: FilterField, values: any[]) => {
            onAddFilter({
                field,
                operator: getDefaultFieldOperatorType(field),
                values,
                defaultValueOptions: [],
            });
        },
        [onAddFilter],
    );

    const items: ItemType[] = useMemo(() => {
        return fields.map((field) => {
            return {
                key: field.field,
                title: field.displayName,
                type: 'item',
                children: [
                    {
                        key: `${field.field}-children`,
                        type: 'custom',
                        render: () => (
                            <ValueMenu
                                field={field}
                                values={[]}
                                defaultOptions={[]}
                                includeCount={includeCount}
                                onChangeValues={(values: any[]) => handleAddFilter(field, values)}
                                isRenderedInSubMenu
                            />
                        ),
                    },
                ],
            };
        });
    }, [fields, includeCount, handleAddFilter]);

    return (
        <Menu items={items}>
            <AddFilterButton variant="text">
                <Icon icon={Plus} size="md" />
                Add filter
            </AddFilterButton>
        </Menu>
    );
}
