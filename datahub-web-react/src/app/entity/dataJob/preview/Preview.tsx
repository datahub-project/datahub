import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { ClockCircleOutlined } from '@ant-design/icons';

import { Deprecation, Domain, EntityType, GlobalTags, Owner, SearchInsight } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { capitalizeFirstLetter } from '../../../shared/textUtil';
import { IconStyleType } from '../../Entity';
import { ANTD_GRAY } from '../../shared/constants';
import { toRelativeTimeString } from '../../../shared/time/timeUtils';

const StatText = styled(Typography.Text)`
    color: ${ANTD_GRAY[8]};
`;

export const Preview = ({
    urn,
    name,
    description,
    platformName,
    platformLogo,
    platformInstanceId,
    owners,
    domain,
    deprecation,
    globalTags,
    snippet,
    insights,
    lastRunTimeMs,
    externalUrl,
}: {
    urn: string;
    name: string;
    description?: string | null;
    platformName: string;
    platformLogo?: string | null;
    platformInstanceId?: string;
    owners?: Array<Owner> | null;
    domain?: Domain | null;
    deprecation?: Deprecation | null;
    globalTags?: GlobalTags | null;
    snippet?: React.ReactNode | null;
    insights?: Array<SearchInsight> | null;
    lastRunTimeMs?: number | null;
    externalUrl?: string | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const capitalizedPlatform = capitalizeFirstLetter(platformName);
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.DataJob, urn)}
            name={name}
            urn={urn}
            description={description || ''}
            type="Data Task"
            typeIcon={entityRegistry.getIcon(EntityType.DataJob, 14, IconStyleType.ACCENT)}
            platform={capitalizedPlatform}
            logoUrl={platformLogo || ''}
            platformInstanceId={platformInstanceId}
            owners={owners}
            tags={globalTags || undefined}
            domain={domain}
            snippet={snippet}
            deprecation={deprecation}
            dataTestID="datajob-item-preview"
            insights={insights}
            externalUrl={externalUrl}
            subHeader={
                (lastRunTimeMs && [
                    <StatText>
                        <ClockCircleOutlined style={{ paddingRight: 8 }} />
                        Last run {toRelativeTimeString(lastRunTimeMs)}
                    </StatText>,
                ]) ||
                undefined
            }
        />
    );
};
