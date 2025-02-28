import { LoadingOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { Dataset } from '../../../../../types.generated';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import FailingEntity from './FailingEntity';
import { getNumAssertionsFailing } from './utils';

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
