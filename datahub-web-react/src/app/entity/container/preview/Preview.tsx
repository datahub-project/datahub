import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { IconStyleType } from '@app/entity/Entity';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import DefaultPreviewCard from '@app/preview/DefaultPreviewCard';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';

import {
    Container,
    DataProduct,
    Deprecation,
    Domain,
    EntityPath,
    EntityType,
    GlobalTags,
    GlossaryTerms,
    Owner,
    ParentContainersResult,
    SearchInsight,
    SubTypes,
} from '@types';

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
    dataProduct,
    parentContainers,
    externalUrl,
    deprecation,
    degree,
    paths,
}: {
    urn: string;
    name: string;
    platformName?: string;
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
    dataProduct?: DataProduct | null;
    deprecation?: Deprecation | null;
    parentContainers?: ParentContainersResult | null;
    externalUrl?: string | null;
    degree?: number;
    paths?: EntityPath[];
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const typeName = capitalizeFirstLetterOnly(subTypes?.typeNames?.[0]) || 'Container';
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
            dataProduct={dataProduct}
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
            degree={degree}
            paths={paths}
        />
    );
};
