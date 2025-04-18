import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { ClockCircleOutlined } from '@ant-design/icons';

import {
    DataProduct,
    Deprecation,
    Domain,
    EntityPath,
    EntityType,
    GlobalTags,
    Health,
    Owner,
    ParentContainersResult,
    SearchInsight,
} from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType } from '../../Entity';
import { ANTD_GRAY } from '../../shared/constants';
import { toRelativeTimeString } from '../../../shared/time/timeUtils';

const StatText = styled(Typography.Text)`
    color: ${ANTD_GRAY[8]};
`;

export const Preview = ({
    urn,
    name,
    subType,
    description,
    platformName,
    platformLogo,
    platformInstanceId,
    owners,
    domain,
    dataProduct,
    deprecation,
    globalTags,
    snippet,
    insights,
    lastRunTimeMs,
    externalUrl,
    degree,
    paths,
    health,
    parentContainers,
}: {
    urn: string;
    name: string;
    subType?: string | null;
    description?: string | null;
    platformName: string;
    platformLogo?: string | null;
    platformInstanceId?: string;
    owners?: Array<Owner> | null;
    domain?: Domain | null;
    dataProduct?: DataProduct | null;
    deprecation?: Deprecation | null;
    globalTags?: GlobalTags | null;
    snippet?: React.ReactNode | null;
    insights?: Array<SearchInsight> | null;
    lastRunTimeMs?: number | null;
    externalUrl?: string | null;
    degree?: number;
    paths?: EntityPath[];
    health?: Health[] | null;
    parentContainers?: ParentContainersResult | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.DataJob, urn)}
            name={name}
            urn={urn}
            description={description || ''}
            type={subType || 'Data Task'}
            typeIcon={entityRegistry.getIcon(EntityType.DataJob, 14, IconStyleType.ACCENT)}
            platform={platformName}
            logoUrl={platformLogo || ''}
            platformInstanceId={platformInstanceId}
            owners={owners}
            tags={globalTags || undefined}
            domain={domain}
            dataProduct={dataProduct}
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
            degree={degree}
            paths={paths}
            health={health || undefined}
            parentContainers={parentContainers}
        />
    );
};
