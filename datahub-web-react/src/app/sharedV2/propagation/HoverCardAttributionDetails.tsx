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
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { toRelativeTimeString } from '@app/shared/time/timeUtils';
import { AttributionDetails } from '@app/sharedV2/propagation/types';
import { PropagationContext, usePropagationContextEntities } from '@app/sharedV2/tags/usePropagationContextEntities';

const DetailsWrapper = styled.div<{ $addMargin?: boolean }>`
    ${({ $addMargin }) => $addMargin && 'margin-top: 8px;'}
`;
interface Props {
    propagationDetails?: AttributionDetails;
    addMargin?: boolean;
}

export default function HoverCardAttributionDetails({ propagationDetails, addMargin }: Props) {
    const { t } = useTranslation('shared.propagation');
    const sourceDetailEntries = propagationDetails?.attribution?.sourceDetail;
    const sourceDetail = usePropagationDetails(sourceDetailEntries);
    let context: PropagationContext | null = null;
    if (propagationDetails?.context) {
        try {
            context = JSON.parse(propagationDetails.context) as PropagationContext;
        } catch (e) {
            console.warn('Failed to parse propagation context as JSON:', propagationDetails.context, e);
        }
    }
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
            <DetailsWrapper $addMargin={addMargin}>
                <Text color="gray" weight="bold" colorLevel={600} size="sm">
                    {t('ingested')}
                </Text>
                {time && (
                    <Text color="gray" colorLevel={600}>
                        {capitalizeFirstLetterOnly(toRelativeTimeString(time))}
                    </Text>
                )}
                {origin && (
                    <div>
                        <Text color="gray" weight="bold" colorLevel={1700} size="sm">
                            {t('sourceLabel')}
                        </Text>
                        <Text color="gray" colorLevel={600}>
                            {origin}
                        </Text>
                    </div>
                )}
            </DetailsWrapper>
        );
    }

    const originEntity = sourceDetail.origin.entity || contextEntities.originEntity;
    const viaEntity = sourceDetail.via.entity;

    return (
        <DetailsWrapper $addMargin={addMargin}>
            <Text color="gray" weight="bold" colorLevel={600} size="sm">
                {t('propagated')}
            </Text>
            {time && (
                <>
                    <Text color="gray" colorLevel={600}>
                        {capitalizeFirstLetterOnly(toRelativeTimeString(time))}
                    </Text>
                </>
            )}
            {originEntity && (
                <div>
                    <Text color="gray" weight="bold" colorLevel={1700} size="sm">
                        {t('origin')}
                    </Text>
                    <AutoCompleteEntityItem entity={originEntity} hideType padding="4px 4px 4px 0" />
                </div>
            )}
            {viaEntity && (
                <div>
                    <Text color="gray" weight="bold" colorLevel={1700} size="sm">
                        {t('via')}
                    </Text>
                    <AutoCompleteEntityItem entity={viaEntity} hideType padding="4px 4px 4px 0" />
                </div>
            )}
        </DetailsWrapper>
    );
}
