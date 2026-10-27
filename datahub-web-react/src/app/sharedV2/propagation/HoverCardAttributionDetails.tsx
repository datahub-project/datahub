import { Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import {
    formatAttributionOrigin,
    getAttributionOrigin,
    isExternal,
    usePropagationDetails,
} from '@app/entity/shared/propagation/utils';
import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';
import HoverCardSection from '@app/sharedV2/hoverCard/HoverCardSection';
import { AttributionDetails } from '@app/sharedV2/propagation/types';
import { parsePropagationContext } from '@app/sharedV2/propagation/utils';
import { usePropagationContextEntities } from '@app/sharedV2/tags/usePropagationContextEntities';
import { capitalizeFirstLetterOnly } from '@app/sharedV2/textUtil';
import { toRelativeTimeString } from '@app/sharedV2/time/timeUtils';

const DetailsWrapper = styled.div<{ $addMargin?: boolean }>`
    ${({ $addMargin }) => $addMargin && 'margin-top: 8px;'}
`;

// Matches the 8px between-section gap of `EntityHoverCard` so Ingested/Source read as hover card sections.
const IngestedWrapper = styled(DetailsWrapper)`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

interface Props {
    propagationDetails?: AttributionDetails;
    addMargin?: boolean;
}

export default function HoverCardAttributionDetails({ propagationDetails, addMargin }: Props) {
    const { t } = useTranslation('shared.propagation');
    const sourceDetailEntries = propagationDetails?.attribution?.sourceDetail;
    const sourceDetail = usePropagationDetails(sourceDetailEntries);
    const context = parsePropagationContext(propagationDetails?.context);
    const contextEntities = usePropagationContextEntities(context);
    const isPropagated = sourceDetail.isPropagated || context?.propagated;
    const external = isExternal(sourceDetailEntries);
    const time = propagationDetails?.attribution?.time;

    if (!isPropagated && !external) return null;

    // Externally-ingested (e.g. Lake Formation) tags are not propagated; their
    // origin is a source marker string, not a resolvable entity, so it is shown
    // as plain text rather than an entity link.
    if (!isPropagated && external) {
        const origin = formatAttributionOrigin(getAttributionOrigin(sourceDetailEntries));
        return (
            <IngestedWrapper $addMargin={addMargin}>
                <HoverCardSection title={t('ingested')}>
                    {time && <Text>{capitalizeFirstLetterOnly(toRelativeTimeString(time))}</Text>}
                </HoverCardSection>
                {origin && (
                    <HoverCardSection title={t('sourceLabel')}>
                        <Text>{origin}</Text>
                    </HoverCardSection>
                )}
            </IngestedWrapper>
        );
    }

    const originEntity = sourceDetail.origin.entity || contextEntities.originEntity;
    const viaEntity = sourceDetail.via.entity;

    return (
        <DetailsWrapper $addMargin={addMargin} data-testid="docPropagationIndicator">
            <Text color="text" weight="bold" size="md">
                {t('propagated')}
            </Text>
            {time && <Text color="textSecondary">{capitalizeFirstLetterOnly(toRelativeTimeString(time))}</Text>}
            {originEntity && (
                <div>
                    <Text color="text" weight="bold" size="md">
                        {t('origin')}
                    </Text>
                    <AutoCompleteEntityItem entity={originEntity} hideType padding="4px 4px 4px 0" />
                </div>
            )}
            {viaEntity && (
                <div>
                    <Text color="text" weight="bold" size="md">
                        {t('via')}
                    </Text>
                    <AutoCompleteEntityItem entity={viaEntity} hideType padding="4px 4px 4px 0" />
                </div>
            )}
        </DetailsWrapper>
    );
}
