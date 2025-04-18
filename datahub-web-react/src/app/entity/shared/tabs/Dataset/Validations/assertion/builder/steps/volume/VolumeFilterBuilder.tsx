import React from 'react';
import styled from 'styled-components';
import { Input, Typography } from 'antd';
import {
    DatasetFilter,
    DatasetFilterType,
    DatasetVolumeSourceType,
} from '../../../../../../../../../../types.generated';

const Form = styled.div`
    margin-top: 16px;
`;

type Props = {
    value?: DatasetFilter | null;
    onChange: (newFilter?: DatasetFilter) => void;
    sourceType: DatasetVolumeSourceType;
    disabled?: boolean;
};

export const VolumeFilterBuilder = ({ value, onChange, sourceType, disabled }: Props) => {
    const updateSqlFilter = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
        const { value: sql } = e.target;
        const newFilter = sql ? { type: DatasetFilterType.Sql, sql } : undefined;
        onChange(newFilter);
    };

    return sourceType === DatasetVolumeSourceType.Query ? (
        <Form>
            <Typography.Title level={5}>Additional Filters (Optional)</Typography.Title>
            <Typography.Paragraph type="secondary">
                Include a custom SQL fragment to scope the freshness check down to a particular segment of data.
                <br />
                This clause will be included as a WHERE clause in the final query used to check this dataset&apos;s
                freshness.
            </Typography.Paragraph>
            <Input.TextArea
                value={value?.sql || ''}
                onChange={updateSqlFilter}
                placeholder={`foo = "FOO_VALUE" and bar = "BAR_VALUE"`}
                disabled={disabled}
            />
        </Form>
    ) : null;
};
