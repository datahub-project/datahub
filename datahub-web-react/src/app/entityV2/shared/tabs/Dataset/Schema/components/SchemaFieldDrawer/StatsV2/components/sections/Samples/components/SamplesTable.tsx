import { Column, Table } from '@src/alchemy-components';
import React, { useMemo } from 'react';
import { SchemaFieldDataType } from '@src/types.generated';
import SampleValueCell from './SampleValueCell';

interface SamplesTableProps {
    samples: string[];
    fieldType?: SchemaFieldDataType;
    onViewSample?: (sample: string) => void;
    maxItems?: number;
}

export default function SamplesTable({ samples, fieldType, onViewSample, maxItems }: SamplesTableProps) {
    const truncatedSamples = useMemo(() => samples.slice(0, maxItems), [samples, maxItems]);

    const columns: Column<string>[] = useMemo(() => {
        const columnsToShow: Column<string>[] = [
            {
                title: 'Sample Values',
                key: 'sample-value',
                render: (record) => {
                    return <SampleValueCell sample={record} onViewSample={onViewSample} />;
                },
                sorter: (sourceA, sourceB) => {
                    if (fieldType !== SchemaFieldDataType.Number) return sourceA.localeCompare(sourceB);

                    try {
                        return Number(sourceA) - Number(sourceB);
                    } catch {
                        return sourceA.localeCompare(sourceB);
                    }
                },
            },
        ];

        return columnsToShow;
    }, [fieldType, onViewSample]);

    return <Table data={truncatedSamples} isScrollable columns={columns} />;
}
