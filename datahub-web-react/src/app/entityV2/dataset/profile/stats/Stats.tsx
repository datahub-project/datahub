import React, { useState } from 'react';
import { Radio } from 'antd';
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
    /**
     * Determines which view should be visible: latest or historical.
     */
    const [view, setView] = useState(ViewType.LATEST);

    const onChangeView = (e) => {
        setView(e.target.value);
    };

    const toggleView = (
        <Radio.Group value={view} onChange={onChangeView}>
            <Radio.Button value={ViewType.LATEST}>Latest</Radio.Button>
            <Radio.Button value={ViewType.HISTORICAL}>Historical</Radio.Button>
        </Radio.Group>
    );

    return (
        <>
            {view === ViewType.LATEST && <LatestStatsView profile={profile} toggleView={toggleView} />}
            {view === ViewType.HISTORICAL && <HistoricalStatsView urn={urn} toggleView={toggleView} />}
        </>
    );
}
