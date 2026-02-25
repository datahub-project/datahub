import React from 'react';

import SamplesTable from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/Samples/components/SamplesTable';
import { Drawer } from '@src/alchemy-components';
import { SchemaFieldDataType } from '@src/types.generated';

interface SampleDrawerProps {
    samples: string[];
    fieldType?: SchemaFieldDataType;
    onViewSample?: (sample: string) => void;
    onBack?: () => void;
    open?: boolean;
}

export default function AllSamplesDrawer({ samples, fieldType, onBack, onViewSample, open }: SampleDrawerProps) {
    return (
        <Drawer title="Sample Values" open={open} closable={false} onBack={onBack} maskTransparent width={560}>
            <SamplesTable samples={samples} fieldType={fieldType} onViewSample={onViewSample} />
        </Drawer>
    );
}
