/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SelectOption, SimpleSelect } from '@components';
import React from 'react';

import { SelectSkeleton } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/SelectSkeleton';

type TypesSelectProps = {
    options: SelectOption[];
    values: string[];
    loading: boolean;
    onUpdate: (values: string[]) => void;
};

export default function TypesSelect({ options, values, loading, onUpdate }: TypesSelectProps) {
    if (loading) return <SelectSkeleton active />;

    // Hide TypesSelect when there are only one option or less
    if (options.length < 2) return null;

    return (
        <SimpleSelect
            dataTestId="types-select"
            placeholder="Change Type"
            selectLabelProps={{ variant: 'labeled', label: 'Change Type' }}
            options={options}
            values={values}
            onUpdate={onUpdate}
            width="full"
            showClear={false}
            isMultiSelect
        />
    );
}
