import React from 'react';

import SampleValueDetailed from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/Samples/components/SampleValueDetailed';
import { Drawer } from '@src/alchemy-components';

interface SampleDrawerProps {
    sample: string;
    onBack?: () => void;
    open?: boolean;
}

export default function SampleDrawer({ sample, onBack, open }: SampleDrawerProps) {
    return (
        <Drawer title="Sample Value" open={open} closable={false} onBack={onBack} maskTransparent width={560}>
            <SampleValueDetailed sample={sample} />
        </Drawer>
    );
}
