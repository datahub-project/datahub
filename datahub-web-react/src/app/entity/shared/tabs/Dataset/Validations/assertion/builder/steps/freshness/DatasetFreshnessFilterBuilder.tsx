import React from 'react';
import styled from 'styled-components';
import { Input, Typography } from 'antd';
import {
    DatasetFilter,
    DatasetFilterType,
    DatasetFreshnessSourceType,
} from '../../../../../../../../../../types.generated';

const Form = styled.div`
    margin-top: 16px;
`;

type Props = {
    value?: DatasetFilter | null;
    onChange: (newFilter?: DatasetFilter) => void;
    sourceType?: DatasetFreshnessSourceType | null;
};

export const DatasetFreshnessFilterBuilder = ({ value, onChange, sourceType }: Props) => {
    const updateSlaFilter = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
        const { value: sql } = e.target;
        const newFilter = sql ? { type: DatasetFilterType.Sql, sql } : undefined;
        onChange(newFilter);
    };

    return sourceType === DatasetFreshnessSourceType.FieldValue ? (
        <Form>
            <Typography.Title level={5}>Additional Filters</Typography.Title>
            <Typography.Paragraph type="secondary">
                Write a custom SQL fragment which will be included as a WHERE clause in the final query used to check
                this dataset&apos;s freshness.
            </Typography.Paragraph>
            <Input.TextArea
                value={value?.sql || ''}
                onChange={updateSlaFilter}
                placeholder={`foo = "FOO_VALUE" and bar = "BAR_VALUE"`}
            />
        </Form>
    ) : null;
};
