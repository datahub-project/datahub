/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import FilterOption from '@app/searchV2/filters/FilterOption';
import { FilterOptionType } from '@app/searchV2/filters/types';
import { getFilterIconAndLabel } from '@app/searchV2/filters/utils';
import { EntityRegistry } from '@src/entityRegistryContext';

export interface DisplayedFilterOption {
    key: string;
    label: React.ReactNode;
    style: any;
    displayName?: string | null;
    nestedOptions?: FilterOptionType[];
}

interface CreateFilterOptionProps {
    filterOption: FilterOptionType;
    entityRegistry: EntityRegistry;
    selectedFilterOptions: FilterOptionType[];
    setSelectedFilterOptions: (values: FilterOptionType[]) => void;
    nestedOptions?: FilterOptionType[];
    includeCount?: boolean;
}

export function mapFilterOption({
    filterOption,
    entityRegistry,
    selectedFilterOptions,
    setSelectedFilterOptions,
    nestedOptions,
    includeCount = true,
}: CreateFilterOptionProps): DisplayedFilterOption {
    const { label: displayName } = getFilterIconAndLabel(
        filterOption.field,
        filterOption.value,
        entityRegistry,
        filterOption.entity || null,
        undefined,
        filterOption.displayName,
    );

    return {
        key: filterOption.value,
        label: (
            <FilterOption
                filterOption={filterOption}
                selectedFilterOptions={selectedFilterOptions}
                setSelectedFilterOptions={setSelectedFilterOptions}
                nestedOptions={nestedOptions}
                includeCount={includeCount}
            />
        ),
        style: { padding: 0 },
        displayName: displayName as string,
        nestedOptions,
    };
}
