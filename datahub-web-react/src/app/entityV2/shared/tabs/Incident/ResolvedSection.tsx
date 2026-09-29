import React from 'react';
import { useTranslation } from 'react-i18next';
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
    const { t } = useTranslation('entity.profile.incident');
    const entityRegistry = useEntityRegistry();
    const history = useHistory();

    const navigateToResolverProfile = () => {
        history.push(entityRegistry.getEntityUrl(EntityType.CorpUser, resolverUrn));
    };

    return (
        <ResolverInfoContainer>
            <ResolverTitleContainer>{t('resolution.sectionTitle')}</ResolverTitleContainer>
            <ResolverDetailsContainer>
                <ResolverSubTitleContainer>
                    <ResolverSubTitle>{t('resolution.resolvedBy')}</ResolverSubTitle>
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
                    <ResolverSubTitle>{t('resolution.noteLabel')}</ResolverSubTitle>
                    <ResolverDetails>{resolverMessage || '-'}</ResolverDetails>
                </ResolverSubTitleContainer>
                <ResolverSubTitleContainer>
                    <ResolverSubTitle>{t('resolution.resolvedDateTime')}</ResolverSubTitle>
                    <ResolverDetails>{getFormattedDateForResolver(resolvedDateAndTime)}</ResolverDetails>
                </ResolverSubTitleContainer>
            </ResolverDetailsContainer>
        </ResolverInfoContainer>
    );
};
