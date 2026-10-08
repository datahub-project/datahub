import { Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { usePropagationDetails } from '@app/entity/shared/propagation/utils';
import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { toRelativeTimeString } from '@app/shared/time/timeUtils';
import { AttributionDetails } from '@app/sharedV2/propagation/types';
import { parsePropagationContext } from '@app/sharedV2/propagation/utils';
import { usePropagationContextEntities } from '@app/sharedV2/tags/usePropagationContextEntities';

const DetailsWrapper = styled.div<{ $addMargin?: boolean }>`
    ${({ $addMargin }) => $addMargin && 'margin-top: 8px;'}
`;
interface Props {
    propagationDetails?: AttributionDetails;
    addMargin?: boolean;
}

export default function HoverCardAttributionDetails({ propagationDetails, addMargin }: Props) {
    const { t } = useTranslation('shared.propagation');
    const sourceDetail = usePropagationDetails(propagationDetails?.attribution?.sourceDetail);
    const context = parsePropagationContext(propagationDetails?.context);
    const contextEntities = usePropagationContextEntities(context);
    const isPropagated = sourceDetail.isPropagated || context?.propagated;

    if (!isPropagated) return null;

    const originEntity = sourceDetail.origin.entity || contextEntities.originEntity;
    const viaEntity = sourceDetail.via.entity;
    const time = propagationDetails?.attribution?.time;

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
