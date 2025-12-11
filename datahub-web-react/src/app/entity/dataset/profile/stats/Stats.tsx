/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Radio } from 'antd';
import React, { useState } from 'react';

import HistoricalStatsView from '@app/entity/dataset/profile/stats/historical/HistoricalStatsView';
import LatestStatsView from '@app/entity/dataset/profile/stats/snapshot/LatestStatsView';

import { DatasetProfile } from '@types';

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
