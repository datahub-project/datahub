import { Input, Select } from 'antd';
import React, { useState } from 'react';

import { checkIsPoolInDataHubCloud, getDisplayablePoolId } from '@app/ingest/executor_saas/utils';
import { colors } from '@src/alchemy-components';
import { useAppConfig } from '@src/app/useAppConfig';
import {
    useGetDefaultRemoteExecutorPoolQuery,
    useListRemoteExecutorPoolsQuery,
} from '@src/graphql/remote_executor.saas.generated';
import { RemoteExecutorPool } from '@src/types.generated';

type Props = {
    value?: string | null;
    onChange?: (value: string) => void;
    onBlur?: (value: string) => void;
    placeholder?: string;
};

export default function RemoteExecutorPoolSelector({ value, onChange, onBlur, placeholder = 'Select a pool' }: Props) {
    const { config } = useAppConfig();
    const isExecutorPoolEnabled = config?.featureFlags?.displayExecutorPools;

    const [searchText, setSearchText] = useState('');

    const { data, loading } = useListRemoteExecutorPoolsQuery({
        variables: {
            query: searchText,
            count: 50,
            start: 0,
        },
        fetchPolicy: 'no-cache',
    });
    const { data: defaultPool } = useGetDefaultRemoteExecutorPoolQuery();

    const pools = (data?.listRemoteExecutorPools?.remoteExecutorPools || []) as RemoteExecutorPool[];
    const total = data?.listRemoteExecutorPools?.total;
    const defaultPoolId = defaultPool?.defaultRemoteExecutorPool?.pool?.executorPoolId || pools[0]?.executorPoolId;

    const handleChange = (newValue: string) => {
        onChange?.(newValue);
        setSearchText('');
    };

    const handleBlur = () => {
        if (value) {
            onBlur?.(value.trim());
            setSearchText('');
        }
    };

    const handleSearch = (text: string) => {
        setSearchText(text);
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
            key={defaultPoolId}
            showSearch
            value={value ?? defaultPoolId}
            placeholder={placeholder}
            onChange={handleChange}
            onBlur={handleBlur}
            onSearch={handleSearch}
            loading={loading}
            filterOption={false}
            defaultValue={defaultPoolId}
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
