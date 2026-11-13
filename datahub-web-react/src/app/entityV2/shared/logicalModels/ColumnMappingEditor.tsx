import React, { useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { SimpleSelect } from '@src/alchemy-components';

export type ColumnMapping = { parentFieldPath: string; childFieldPath: string };

const Row = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
`;

const ParentLabel = styled.div`
    width: 180px;
    font-weight: 600;
`;

const NONE = '__none__';

type Props = {
    parentColumns: string[];
    childColumns: string[];
    value: ColumnMapping[];
    onChange: (mappings: ColumnMapping[]) => void;
};

export default function ColumnMappingEditor({ parentColumns, childColumns, value, onChange }: Props) {
    const { t } = useTranslation('logicalModels');

    // Auto-prefill exact (case-insensitive) matches the first time we have empty value.
    useEffect(() => {
        if (value.length === 0 && parentColumns.length > 0) {
            const auto: ColumnMapping[] = [];
            parentColumns.forEach((parent) => {
                const match = childColumns.find((child) => child.toLowerCase() === parent.toLowerCase());
                if (match) auto.push({ parentFieldPath: parent, childFieldPath: match });
            });
            if (auto.length > 0) onChange(auto);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [parentColumns, childColumns]);

    const childFor = (parent: string) =>
        value.find((mapping) => mapping.parentFieldPath === parent)?.childFieldPath ?? NONE;

    const setMapping = (parent: string, child: string) => {
        const without = value.filter((mapping) => mapping.parentFieldPath !== parent);
        onChange(child === NONE ? without : [...without, { parentFieldPath: parent, childFieldPath: child }]);
    };

    return (
        <div>
            {parentColumns.map((parent) => (
                <Row key={parent}>
                    <ParentLabel>{parent}</ParentLabel>
                    <SimpleSelect
                        width={220}
                        values={[childFor(parent)]}
                        onUpdate={(vals) => setMapping(parent, vals[0] ?? NONE)}
                        showClear={false}
                        dataTestId={`mapping-select-${parent}`}
                        options={[
                            { value: NONE, label: t('mappingEditor.notMapped') },
                            ...childColumns.map((child) => ({ value: child, label: child })),
                        ]}
                    />
                </Row>
            ))}
        </div>
    );
}
