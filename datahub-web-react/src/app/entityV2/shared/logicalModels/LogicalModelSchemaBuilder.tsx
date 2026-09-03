import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { LogicalModelColumnDraft } from '@app/entityV2/shared/logicalModels/logicalModels.types';
import { LOGICAL_MODEL_COLUMN_TYPE_OPTIONS } from '@app/entityV2/shared/logicalModels/logicalModels.utils';
import { Button, Input, SimpleSelect } from '@src/alchemy-components';

import { SchemaFieldDataType } from '@types';

const Row = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
`;

type Props = {
    columns: LogicalModelColumnDraft[];
    onChange: (columns: LogicalModelColumnDraft[]) => void;
};

export default function LogicalModelSchemaBuilder({ columns, onChange }: Props) {
    const { t } = useTranslation('logicalModels');
    const update = (index: number, patch: Partial<LogicalModelColumnDraft>) => {
        onChange(columns.map((col, i) => (i === index ? { ...col, ...patch } : col)));
    };
    const addRow = () => onChange([...columns, { fieldPath: '', type: SchemaFieldDataType.String }]);
    const removeRow = (index: number) => onChange(columns.filter((_, i) => i !== index));

    return (
        <div>
            {columns.map((col, index) => (
                // eslint-disable-next-line react/no-array-index-key
                <Row key={index}>
                    <Input
                        label=""
                        placeholder={t('schemaBuilder.columnNamePlaceholder')}
                        value={col.fieldPath}
                        setValue={(v) => update(index, { fieldPath: v })}
                    />
                    <SimpleSelect
                        width={140}
                        values={[col.type]}
                        onUpdate={(vals) => vals[0] && update(index, { type: vals[0] as SchemaFieldDataType })}
                        showClear={false}
                        options={LOGICAL_MODEL_COLUMN_TYPE_OPTIONS.map((type) => ({ value: type, label: type }))}
                    />
                    <Button
                        variant="text"
                        icon={{ icon: X }}
                        onClick={() => removeRow(index)}
                        data-testid="remove-logical-column"
                    />
                </Row>
            ))}
            <Button variant="text" icon={{ icon: Plus }} onClick={addRow} data-testid="add-logical-column">
                {t('schemaBuilder.addColumn')}
            </Button>
        </div>
    );
}
