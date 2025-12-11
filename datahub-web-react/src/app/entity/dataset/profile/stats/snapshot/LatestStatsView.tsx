/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Affix, Row, Typography } from 'antd';
import React, { ReactNode } from 'react';
import styled from 'styled-components';

import DataProfileView from '@app/entity/dataset/profile/stats/snapshot/SnapshotStatsView';

import { DatasetProfile } from '@types';

const HeaderRow = styled(Row)`
    padding-top: 24px;
    padding-bottom: 28px;
    background-color: white;
`;

export type Props = {
    profile: DatasetProfile;
    toggleView: ReactNode;
};

export default function LatestStatsView({ profile, toggleView }: Props) {
    const reportedAtDate = new Date(profile.timestampMillis);
    return (
        <>
            <Affix offsetTop={127}>
                <HeaderRow justify="space-between" align="middle">
                    <div>
                        <Typography.Title level={2}>Latest Stats</Typography.Title>
                        <Typography.Text style={{ color: 'gray' }}>
                            Reported on {reportedAtDate.toLocaleDateString()} at {reportedAtDate.toLocaleTimeString()}
                        </Typography.Text>
                    </div>
                    {toggleView}
                </HeaderRow>
            </Affix>
            <DataProfileView profile={profile} />
        </>
    );
}
