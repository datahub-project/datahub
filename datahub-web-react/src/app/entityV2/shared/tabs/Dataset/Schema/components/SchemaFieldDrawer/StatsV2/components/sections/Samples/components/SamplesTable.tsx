import React, { useMemo } from 'react';

import SampleValueCell from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/Samples/components/SampleValueCell';
import { Column, Table } from '@src/alchemy-components';
import { SchemaFieldDataType } from '@src/types.generated';

interface SamplesTableProps {
    samples: string[];
    fieldType?: SchemaFieldDataType;
    onViewSample?: (sample: string) => void;
    maxItems?: number;
}

export default function SamplesTable({ samples, fieldType, onViewSample, maxItems }: SamplesTableProps) {
    const truncatedSamples = useMemo(() => samples.slice(0, maxItems), [samples, maxItems]);

    const columns: Column<string>[] = useMemo(() => {
        const isPreviewMode = maxItems !== undefined;
        const columnsToShow: Column<string>[] = [
            {
                title: 'Sample Values',
                key: 'sample-value',
                render: (record) => {
                    return <SampleValueCell sample={record} onViewSample={onViewSample} />;
                },
                sorter: isPreviewMode
                    ? undefined
                    : (sourceA, sourceB) => {
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
    }, [fieldType, onViewSample, maxItems]);

    return <Table data={truncatedSamples} isScrollable columns={columns} />;
}
