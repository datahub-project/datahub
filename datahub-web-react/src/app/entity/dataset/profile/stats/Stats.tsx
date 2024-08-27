import React, { useState } from 'react';
import { Radio } from 'antd';
import { useTranslation } from 'react-i18next';
import { DatasetProfile } from '../../../../../types.generated';
import LatestStatsView from './snapshot/LatestStatsView';
import HistoricalStatsView from './historical/HistoricalStatsView';

export type Props = {
    urn: string;
    profile: DatasetProfile;
};

enum ViewType {
    LATEST,
    HISTORICAL,
}

export default function Stats({ urn, profile }: Props) {
    const { t } = useTranslation();
    /**
     * Determines which view should be visible: latest or historical.
     */
    const [view, setView] = useState(ViewType.LATEST);

    const onChangeView = (e) => {
        setView(e.target.value);
    };

    const toggleView = (
        <Radio.Group value={view} onChange={onChangeView}>
            <Radio.Button value={ViewType.LATEST}>{t('common.latest')}</Radio.Button>
            <Radio.Button value={ViewType.HISTORICAL}>{t('common.historical')}</Radio.Button>
        </Radio.Group>
    );

    return (
        <>
            {view === ViewType.LATEST && <LatestStatsView profile={profile} toggleView={toggleView} />}
            {view === ViewType.HISTORICAL && <HistoricalStatsView urn={urn} toggleView={toggleView} />}
        </>
    );
}
