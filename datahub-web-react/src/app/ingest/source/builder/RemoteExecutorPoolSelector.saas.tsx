import { Input, Select } from 'antd';
import React from 'react';
import { debounce } from 'remirror';

import { checkIsPoolInDataHubCloud, getDisplayablePoolId } from '@app/ingest/executor_saas/utils';
import { colors } from '@src/alchemy-components';
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
};

export default function RemoteExecutorPoolSelector({
    value,
    onChange,
    onBlur,
    placeholder = 'Select a pool',
    pools,
    total,
    loading,
    handleSearch,
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
        <Select
            key="remote-executor-selector"
            showSearch
            value={value}
            placeholder={placeholder}
            onChange={handleChange}
            onBlur={handleBlur}
            onSearch={debounce(200, handleSearch)}
            loading={loading}
            filterOption={false}
        >
            {pools.map((pool) => (
                <Select.Option key={pool.executorPoolId} value={pool.executorPoolId}>
                    <div>
                        {getDisplayablePoolId(pool, '')}
                        {checkIsPoolInDataHubCloud(pool) ? (
                            <span style={{ color: colors.blue[600], marginLeft: 4 }}> (Hosted in DataHub Cloud)</span>
                        ) : null}
                        {pool.isDefault ? (
                            <span style={{ opacity: 0.5, fontStyle: 'italic', marginLeft: 4 }}> Default</span>
                        ) : null}
                    </div>
                    {/* Pool description */}
                    <div style={{ opacity: 0.75 }}>{pool.description}</div>
                </Select.Option>
            ))}
            {total && pools.length < total && (
                <Select.Option key="more-label" value="more-label" disabled>
                    {`Showing ${pools.length} of ${total} pools. Please refine your search.`}
                </Select.Option>
            )}
        </Select>
    );
}
