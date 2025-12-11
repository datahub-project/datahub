/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { LoadingOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';

import FailingEntity from '@app/entityV2/shared/embed/UpstreamHealth/FailingEntity';
import { getNumAssertionsFailing } from '@app/entityV2/shared/embed/UpstreamHealth/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Dataset } from '@types';

export const FailingSectionWrapper = styled.div`
    margin: 8px 0 0 34px;
    font-size: 14px;
    color: black;
`;

export const FailingDataWrapper = styled.div`
    margin-left: 20px;
`;

export const LoadMoreButton = styled(Button)`
    border: none;
    padding: 0;
    box-shadow: none;
`;

export const LoadingWrapper = styled.div`
    // set width and height to what our load more button is
    width: 68px;
    height: 32px;
    display: flex;
    justify-content: center;
    align-items: center;
`;

interface Props {
    datasetsWithFailingAssertions: Dataset[];
    totalDatasetsWithFailingAssertions: number;
    fetchMoreAssertionsData: () => void;
    isLoadingAssertions: boolean;
}

export default function FailingAssertions({
    datasetsWithFailingAssertions,
    totalDatasetsWithFailingAssertions,
    fetchMoreAssertionsData,
    isLoadingAssertions,
}: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <FailingSectionWrapper>
            {totalDatasetsWithFailingAssertions} data source{totalDatasetsWithFailingAssertions > 1 && 's'} with failing
            assertions
            <FailingDataWrapper>
                {datasetsWithFailingAssertions.map((dataset) => {
                    const totalNumAssertions = dataset.assertions?.assertions?.length;
                    const numAssertionsFailing = getNumAssertionsFailing(dataset);

                    return (
                        <FailingEntity
                            link={entityRegistry.getEntityUrl(dataset.type, dataset.urn)}
                            displayName={entityRegistry.getDisplayName(dataset.type, dataset)}
                            contentText={`${numAssertionsFailing} of ${totalNumAssertions} failing`}
                        />
                    );
                })}
                {totalDatasetsWithFailingAssertions > datasetsWithFailingAssertions.length && (
                    <>
                        {isLoadingAssertions && (
                            <LoadingWrapper>
                                <LoadingOutlined />
                            </LoadingWrapper>
                        )}
                        {!isLoadingAssertions && (
                            <LoadMoreButton onClick={fetchMoreAssertionsData}>+ Load more</LoadMoreButton>
                        )}
                    </>
                )}
            </FailingDataWrapper>
        </FailingSectionWrapper>
    );
}
