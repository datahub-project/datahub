/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import { useHistory } from 'react-router';

import {
    ResolverDetails,
    ResolverDetailsContainer,
    ResolverInfoContainer,
    ResolverSubTitle,
    ResolverSubTitleContainer,
    ResolverTitleContainer,
} from '@app/entityV2/shared/tabs/Incident/styledComponents';
import { getFormattedDateForResolver } from '@app/entityV2/shared/tabs/Incident/utils';
import { Avatar } from '@src/alchemy-components';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { EntityType } from '@src/types.generated';

type ResolvedSectionProps = {
    resolverUrn: string;
    resolverName: string;
    resolverImageUrl?: string;
    resolverMessage?: string;
    resolvedDateAndTime?: number;
};

export const ResolvedSection = ({
    resolverUrn,
    resolverName,
    resolverMessage,
    resolvedDateAndTime,
    resolverImageUrl,
}: ResolvedSectionProps) => {
    const entityRegistry = useEntityRegistry();
    const history = useHistory();

    const navigateToResolverProfile = () => {
        history.push(entityRegistry.getEntityUrl(EntityType.CorpUser, resolverUrn));
    };

    return (
        <ResolverInfoContainer>
            <ResolverTitleContainer>Incident Resolved</ResolverTitleContainer>
            <ResolverDetailsContainer>
                <ResolverSubTitleContainer>
                    <ResolverSubTitle>Resolved By</ResolverSubTitle>
                    <ResolverDetails>
                        <Avatar
                            name={resolverName}
                            imageUrl={resolverImageUrl}
                            showInPill
                            onClick={navigateToResolverProfile}
                        />
                    </ResolverDetails>
                </ResolverSubTitleContainer>
                <ResolverSubTitleContainer>
                    <ResolverSubTitle>Note</ResolverSubTitle>
                    <ResolverDetails>{resolverMessage || '-'}</ResolverDetails>
                </ResolverSubTitleContainer>
                <ResolverSubTitleContainer>
                    <ResolverSubTitle>Resolved Date and Time</ResolverSubTitle>
                    <ResolverDetails>{getFormattedDateForResolver(resolvedDateAndTime)}</ResolverDetails>
                </ResolverSubTitleContainer>
            </ResolverDetailsContainer>
        </ResolverInfoContainer>
    );
};
