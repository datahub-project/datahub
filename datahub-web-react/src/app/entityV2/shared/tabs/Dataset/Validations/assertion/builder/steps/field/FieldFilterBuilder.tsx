import React from 'react';
import { Input, Typography } from 'antd';
import {
    DatasetFieldAssertionSourceType,
    DatasetFilter,
    DatasetFilterType,
} from '../../../../../../../../../../types.generated';

type Props = {
    value?: DatasetFilter | null;
    onChange: (newFilter?: DatasetFilter) => void;
    sourceType?: DatasetFieldAssertionSourceType | null;
    disabled?: boolean;
};

export const FieldFilterBuilder = ({ value, onChange, sourceType, disabled }: Props) => {
    const updateSqlFilter = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
        const { value: sql } = e.target;
        const newFilter = sql ? { type: DatasetFilterType.Sql, sql } : undefined;
        onChange(newFilter);
    };

    return sourceType !== DatasetFieldAssertionSourceType.DatahubDatasetProfile ? (
        <div>
            <Typography.Title level={5}>Additional Filters (Optional)</Typography.Title>
            <Typography.Paragraph type="secondary">
                Include a custom SQL fragment to scope the column value check down to a particular segment of data.
                <br />
                This clause will be included as a WHERE clause in the final query used to evaluate this assertion.
            </Typography.Paragraph>
            <Input.TextArea
                value={value?.sql || ''}
                onChange={updateSqlFilter}
                placeholder={`foo = "FOO_VALUE" and bar = "BAR_VALUE"`}
                disabled={disabled}
            />
        </div>
    ) : null;
};
