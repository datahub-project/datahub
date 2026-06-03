import React from 'react';
import { useTranslation } from 'react-i18next';

import SampleValueDetailed from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/Samples/components/SampleValueDetailed';
import { Drawer } from '@src/alchemy-components';

interface SampleDrawerProps {
    sample: string;
    onBack?: () => void;
    open?: boolean;
}

export default function SampleDrawer({ sample, onBack, open }: SampleDrawerProps) {
    const { t } = useTranslation('entity.profile.schema');

    return (
        <Drawer
            title={t('statsV2Samples.sampleDrawerTitle')}
            open={open}
            closable={false}
            onBack={onBack}
            maskTransparent
            width={560}
        >
            <SampleValueDetailed sample={sample} />
        </Drawer>
    );
}
