/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { LoadingOutlined } from '@ant-design/icons';
import React from 'react';

import {
    FailingDataWrapper,
    FailingSectionWrapper,
    LoadMoreButton,
    LoadingWrapper,
} from '@app/entityV2/shared/embed/UpstreamHealth/FailingAssertions';
import FailingEntity from '@app/entityV2/shared/embed/UpstreamHealth/FailingEntity';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Dataset } from '@types';

interface Props {
    datasetsWithActiveIncidents: Dataset[];
    totalDatasetsWithActiveIncidents: number;
    fetchMoreIncidentsData: () => void;
    isLoadingIncidents: boolean;
}

export default function ActiveIncidents({
    datasetsWithActiveIncidents,
    totalDatasetsWithActiveIncidents,
    fetchMoreIncidentsData,
    isLoadingIncidents,
}: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <FailingSectionWrapper>
            {totalDatasetsWithActiveIncidents} data source{totalDatasetsWithActiveIncidents > 1 && 's'} with active
            incidents
            <FailingDataWrapper>
                {datasetsWithActiveIncidents.map((dataset) => {
                    const numActiveIncidents = (dataset as any).activeIncidents.total;

                    return (
                        <FailingEntity
                            key={dataset.urn}
                            link={entityRegistry.getEntityUrl(dataset.type, dataset.urn)}
                            displayName={entityRegistry.getDisplayName(dataset.type, dataset)}
                            contentText={`${numActiveIncidents} active incident${numActiveIncidents > 1 ? 's' : ''}`}
                        />
                    );
                })}
                {totalDatasetsWithActiveIncidents > datasetsWithActiveIncidents.length && (
                    <>
                        {isLoadingIncidents && (
                            <LoadingWrapper>
                                <LoadingOutlined />
                            </LoadingWrapper>
                        )}
                        {!isLoadingIncidents && (
                            <LoadMoreButton onClick={fetchMoreIncidentsData}>+ Load more</LoadMoreButton>
                        )}
                    </>
                )}
            </FailingDataWrapper>
        </FailingSectionWrapper>
    );
}
