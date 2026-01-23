import React, { useMemo } from 'react';
import { debounce } from 'remirror';

import { checkIsPoolInDataHubCloud, getDisplayablePoolId } from '@app/ingest/executor_saas/utils';
import { Input, SelectOption, SimpleSelect } from '@src/alchemy-components';
import { useAppConfig } from '@src/app/useAppConfig';
import { RemoteExecutorPool } from '@src/types.generated';

type Props = {
    value?: string | null;
    onChange?: (value: string) => void;
    onBlur?: (value: string) => void;
    placeholder?: string;
    pools: RemoteExecutorPool[];
    total?: number;
    loading?: boolean;
    handleSearch: (text: string) => void;
    visibilityDeps?: React.DependencyList;
};

const HIDDEN_ITEMS_OPTION_VALUE = 'hidden-items-value';

export default function RemoteExecutorPoolSelector({
    value,
    onChange,
    onBlur,
    placeholder = 'Select a pool',
    pools,
    total,
    loading,
    handleSearch,
    visibilityDeps,
}: Props) {
    const { config } = useAppConfig();
    const isExecutorPoolEnabled = config?.featureFlags?.displayExecutorPools;

    const handleChange = (newValue: string) => {
        onChange?.(newValue);
        handleSearch('');
    };

    const handleBlur = () => {
        if (value) {
            onBlur?.(value.trim());
            handleSearch('');
        }
    };

    const options: SelectOption[] = useMemo(() => {
        const poolOptions: SelectOption[] = pools.map((pool) => ({
            value: pool.executorPoolId,
            label: `${getDisplayablePoolId(pool, '')}${checkIsPoolInDataHubCloud(pool) ? ' (Hosted in DataHub Cloud)' : ''}`,
            description: pool.description ?? undefined,
        }));

        if (total && pools.length < total) {
            poolOptions.push({
                label: `Showing ${pools.length} of ${total} pools. Please refine your search.`,
                value: HIDDEN_ITEMS_OPTION_VALUE,
            });
        }

        return poolOptions;
    }, [pools, total]);

    if (!isExecutorPoolEnabled) {
        return (
            <Input
                placeholder={placeholder}
                value={value || ''}
                onChange={(event) => handleChange(event.target.value)}
                onBlur={handleBlur}
            />
        );
    }

    return (
        <SimpleSelect
            key="remote-executor-selector"
            options={options}
            showSearch
            values={value ? [value] : []}
            disabledValues={[HIDDEN_ITEMS_OPTION_VALUE]}
            placeholder={placeholder}
            onUpdate={(values) => handleChange(values[0])}
            onSearchChange={debounce(200, handleSearch)}
            isLoading={loading}
            showClear={false}
            width="full"
            size="lg"
            visibilityDeps={visibilityDeps}
        />
    );
}
