/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
