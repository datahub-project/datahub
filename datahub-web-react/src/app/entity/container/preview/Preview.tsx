import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import {
    Container,
    EntityType,
    Owner,
    SearchInsight,
    SubTypes,
    Domain,
    ParentContainersResult,
    GlobalTags,
    Deprecation,
    GlossaryTerms,
} from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType } from '../../Entity';
import { ANTD_GRAY } from '../../shared/constants';

const StatText = styled(Typography.Text)`
    color: ${ANTD_GRAY[8]};
`;

export const Preview = ({
    urn,
    name,
    platformName,
    platformLogo,
    platformInstanceId,
    description,
    owners,
    tags,
    glossaryTerms,
    insights,
    subTypes,
    logoComponent,
    container,
    entityCount,
    domain,
    parentContainers,
    externalUrl,
    deprecation,
}: {
    urn: string;
    name: string;
    platformName: string;
    platformLogo?: string | null;
    platformInstanceId?: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    tags?: GlobalTags | null;
    glossaryTerms?: GlossaryTerms | null;
    insights?: Array<SearchInsight> | null;
    subTypes?: SubTypes | null;
    logoComponent?: JSX.Element;
    container?: Container | null;
    entityCount?: number;
    domain?: Domain | null;
    deprecation?: Deprecation | null;
    parentContainers?: ParentContainersResult | null;
    externalUrl?: string | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const typeName = (subTypes?.typeNames?.length && subTypes?.typeNames[0]) || 'Container';
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Container, urn)}
            name={name || ''}
            urn={urn}
            platform={platformName}
            platformInstanceId={platformInstanceId}
            description={description || ''}
            type={typeName}
            owners={owners}
            deprecation={deprecation}
            insights={insights}
            logoUrl={platformLogo || undefined}
            logoComponent={logoComponent}
            container={container || undefined}
            typeIcon={entityRegistry.getIcon(EntityType.Container, 12, IconStyleType.ACCENT)}
            domain={domain || undefined}
            parentContainers={parentContainers}
            tags={tags || undefined}
            glossaryTerms={glossaryTerms || undefined}
            externalUrl={externalUrl}
            subHeader={
                (entityCount && [
                    <StatText>
                        <b>{entityCount}</b> {entityCount === 1 ? 'entity' : 'entities'}
                    </StatText>,
                ]) ||
                undefined
            }
        />
    );
};
