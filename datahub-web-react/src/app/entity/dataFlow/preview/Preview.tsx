import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { IconStyleType } from '@app/entity/Entity';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import DefaultPreviewCard from '@app/preview/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

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
} from '@types';

const StatText = styled(Typography.Text)`
    color: ${ANTD_GRAY[8]};
`;

export const Preview = ({
    urn,
    name,
    platformInstanceId,
    description,
    platformName,
    platformLogo,
    owners,
    globalTags,
    domain,
    dataProduct,
    externalUrl,
    snippet,
    insights,
    jobCount,
    deprecation,
    degree,
    paths,
    health,
    parentContainers,
}: {
    urn: string;
    name: string;
    platformInstanceId?: string;
    description?: string | null;
    platformName?: string;
    platformLogo?: string | null;
    owners?: Array<Owner> | null;
    domain?: Domain | null;
    dataProduct?: DataProduct | null;
    globalTags?: GlobalTags | null;
    deprecation?: Deprecation | null;
    externalUrl?: string | null;
    snippet?: React.ReactNode | null;
    insights?: Array<SearchInsight> | null;
    jobCount?: number | null;
    degree?: number;
    paths?: EntityPath[];
    health?: Health[] | null;
    parentContainers?: ParentContainersResult | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.DataFlow, urn)}
            name={name}
            urn={urn}
            description={description || ''}
            platformInstanceId={platformInstanceId}
            type="Data Pipeline"
            typeIcon={entityRegistry.getIcon(EntityType.DataFlow, 14, IconStyleType.ACCENT)}
            platform={platformName}
            logoUrl={platformLogo || ''}
            owners={owners}
            tags={globalTags || undefined}
            domain={domain}
            dataProduct={dataProduct}
            snippet={snippet}
            insights={insights}
            externalUrl={externalUrl}
            deprecation={deprecation}
            subHeader={
                (jobCount && [
                    <StatText>
                        <b>{jobCount}</b> {entityRegistry.getCollectionName(EntityType.DataJob)}
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
