import React from 'react';
import { Drawer } from '@src/alchemy-components';
import SampleValueDetailed from './SampleValueDetailed';

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
