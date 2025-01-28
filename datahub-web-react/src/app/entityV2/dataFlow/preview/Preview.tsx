import { GenericEntityProperties } from '@app/entity/shared/types';
import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
import {
    DataProduct,
    Deprecation,
    Domain,
    EntityPath,
    EntityType,
    GlobalTags,
    Owner,
    ParentContainersResult,
    SearchInsight,
} from '../../../../types.generated';
import DefaultPreviewCard from '../../../previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType, PreviewType } from '../../Entity';
import { ANTD_GRAY } from '../../shared/constants';
import { EntityMenuItems } from '../../shared/EntityDropdown/EntityMenuActions';

const StatText = styled(Typography.Text)`
    color: ${ANTD_GRAY[8]};
`;

export const Preview = ({
    urn,
    data,
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
    isOutputPort,
    headerDropdownItems,
    previewType,
    parentContainers,
}: {
    urn: string;
    data: GenericEntityProperties | null;
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
    isOutputPort?: boolean;
    headerDropdownItems?: Set<EntityMenuItems>;
    previewType?: PreviewType;
    parentContainers?: ParentContainersResult | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.DataFlow, urn)}
            name={name}
            urn={urn}
            data={data}
            description={description || ''}
            platformInstanceId={platformInstanceId}
            entityType={EntityType.DataFlow}
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
            isOutputPort={isOutputPort}
            headerDropdownItems={headerDropdownItems}
            previewType={previewType}
            parentEntities={parentContainers?.containers}
        />
    );
};
