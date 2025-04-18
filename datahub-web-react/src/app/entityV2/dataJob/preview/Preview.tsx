import { GenericEntityProperties } from '@app/entity/shared/types';
import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { ClockCircleOutlined } from '@ant-design/icons';

import {
    BrowsePathV2,
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
import { toRelativeTimeString } from '../../../shared/time/timeUtils';
import { EntityMenuItems } from '../../shared/EntityDropdown/EntityMenuActions';

const StatText = styled(Typography.Text)`
    color: ${ANTD_GRAY[8]};
`;

export const Preview = ({
    urn,
    data,
    name,
    subtype,
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
    isOutputPort,
    headerDropdownItems,
    previewType,
    browsePaths,
    parentContainers,
}: {
    urn: string;
    data: GenericEntityProperties | null;
    name: string;
    subtype?: string | null;
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
    isOutputPort?: boolean;
    headerDropdownItems?: Set<EntityMenuItems>;
    previewType?: PreviewType;
    browsePaths?: BrowsePathV2;
    parentContainers?: ParentContainersResult | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.DataJob, urn)}
            name={name}
            urn={urn}
            data={data}
            description={description || ''}
            entityType={EntityType.DataJob}
            type={subtype}
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
            isOutputPort={isOutputPort}
            headerDropdownItems={headerDropdownItems}
            previewType={previewType}
            browsePaths={browsePaths}
            parentEntities={parentContainers?.containers}
        />
    );
};
