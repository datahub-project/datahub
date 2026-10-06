import React from 'react';
import { useTranslation } from 'react-i18next';

import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';

export default function SampleLargeModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    return <LargeModule {...props}>{t('sampleModule.content')}</LargeModule>;
}
