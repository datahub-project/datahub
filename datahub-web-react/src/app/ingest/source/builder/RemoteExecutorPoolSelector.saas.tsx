import React, { useState } from 'react';
import { Select } from 'antd';
import {
    useGetDefaultRemoteExecutorPoolQuery,
    useListRemoteExecutorPoolsQuery,
} from '@src/graphql/remote_executor.saas.generated';
import { colors } from '@src/alchemy-components';

type Props = {
    value?: string | null;
    onChange?: (value: string) => void;
    onBlur?: (value: string) => void;
    placeholder?: string;
};

export default function RemoteExecutorPoolSelector({ value, onChange, onBlur, placeholder = 'Select a pool' }: Props) {
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

    const pools = data?.listRemoteExecutorPools.remoteExecutorPools || [];
    const total = data?.listRemoteExecutorPools.total;
    const defaultPoolName = defaultPool?.defaultRemoteExecutorPool.pool?.poolName || pools[0]?.poolName;

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

    return (
        <Select
            key={defaultPoolName}
            showSearch
            value={value ?? defaultPoolName}
            placeholder={placeholder}
            onChange={handleChange}
            onBlur={handleBlur}
            onSearch={handleSearch}
            loading={loading}
            filterOption={false}
            defaultValue={defaultPoolName}
        >
            {pools.map((pool) => (
                <Select.Option key={pool.poolName} value={pool.poolName}>
                    <div>
                        {pool.poolName}
                        {pool.remoteExecutors?.remoteExecutors?.find((exec) => exec.executorInternal) ? (
                            <span style={{ color: colors.blue[600], marginLeft: 4 }}> (Hosted in DataHub Cloud)</span>
                        ) : null}
                        {pool.isDefault ? (
                            <span style={{ opacity: 0.5, fontStyle: 'italic', marginLeft: 4 }}> Suggested Default</span>
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
