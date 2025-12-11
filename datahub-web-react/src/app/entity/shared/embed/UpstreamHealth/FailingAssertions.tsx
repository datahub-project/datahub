/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import FailingEntity from '@app/entity/shared/embed/UpstreamHealth/FailingEntity';
import { UpstreamSummary, getNumAssertionsFailing } from '@app/entity/shared/embed/UpstreamHealth/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

const FailingSectionWrapper = styled.div`
    margin: 5px 0 0 34px;
    font-size: 14px;
    color: black;
`;

const FailingDataWrapper = styled.div`
    margin-left: 20px;
`;

interface Props {
    upstreamSummary: UpstreamSummary;
}

export default function FailingAssertions({ upstreamSummary }: Props) {
    const { datasetsWithFailingAssertions } = upstreamSummary;
    const entityRegistry = useEntityRegistry();

    return (
        <FailingSectionWrapper>
            {datasetsWithFailingAssertions.length} data source{datasetsWithFailingAssertions.length > 1 && 's'} with
            failing assertions
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
            </FailingDataWrapper>
        </FailingSectionWrapper>
    );
}
