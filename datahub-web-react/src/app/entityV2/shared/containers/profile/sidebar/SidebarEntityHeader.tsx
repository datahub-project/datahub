import React from 'react';
import styled from 'styled-components';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { IconStyleType } from '@app/entityV2/Entity';
import { DeprecationIcon } from '@app/entityV2/shared/components/styled/DeprecationIcon';
import EntityTitleLoadingSection from '@app/entityV2/shared/containers/profile/header/EntityHeaderLoadingSection';
import EntityName from '@app/entityV2/shared/containers/profile/header/EntityName';
import ContainerIcon from '@app/entityV2/shared/containers/profile/header/PlatformContent/ContainerIcon';
import PlatformHeaderIcons from '@app/entityV2/shared/containers/profile/header/PlatformContent/PlatformHeaderIcons';
import StructuredPropertyBadge from '@app/entityV2/shared/containers/profile/header/StructuredPropertyBadge';
import { getDisplayedEntityType } from '@app/entityV2/shared/containers/profile/header/utils';
import VersioningBadge from '@app/entityV2/shared/versioning/VersioningBadge';
import ContextPath from '@app/previewV2/ContextPath';
import HealthIcon from '@app/previewV2/HealthIcon';
import NotesIcon from '@app/previewV2/NotesIcon';
import useContentTruncation from '@app/shared/useContentTruncation';
import HorizontalScroller from '@app/sharedV2/carousel/HorizontalScroller';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Container, DataPlatform, Entity, EntityType, Post } from '@types';

const TitleContainer = styled(HorizontalScroller)`
    display: flex;
    gap: 5px;
`;

const EntityDetailsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 5px;
    margin-left: 8px;
`;

const NameWrapper = styled.div`
    display: flex;
    gap: 6px;
    align-items: center;

    font-size: 16px;
`;

const SidebarEntityHeader = () => {
    const { urn, entityType, entityData, loading } = useEntityData();
    const refetch = useRefetch();
    const entityRegistry = useEntityRegistry();
    const entityUrl = entityRegistry.getEntityUrl(entityType, entityData?.urn as string);

    const { contentRef, isContentTruncated } = useContentTruncation(entityData);
    const typeIcon =
        entityType === EntityType.Container ? (
            <ContainerIcon container={entityData as Container} />
        ) : (
            entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT)
        );
    const displayedEntityType = getDisplayedEntityType(entityData, entityRegistry, entityType);

    const platform = entityType === EntityType.SchemaField ? entityData?.parent?.platform : entityData?.platform;
    const platforms =
        entityType === EntityType.SchemaField ? entityData?.parent?.siblingPlatforms : entityData?.siblingPlatforms;

    const containerPath =
        entityData?.parentContainers?.containers ||
        entityData?.parentDomains?.domains ||
        entityData?.parentNodes?.nodes ||
        [];
    const parentPath: Entity[] = entityData?.parent ? [entityData.parent as Entity] : [];
    const parentEntities = containerPath.length ? containerPath : parentPath;

    if (loading) {
        return <EntityTitleLoadingSection />;
    }
    return (
        <TitleContainer scrollButtonSize={18} scrollButtonOffset={15}>
            <PlatformHeaderIcons
                platform={platform as DataPlatform}
                platforms={platforms as DataPlatform[]}
                size={24}
            />
            <EntityDetailsContainer>
                <NameWrapper>
                    <EntityName isNameEditable={false} />
                    {!!entityData?.notes?.total && (
                        <NotesIcon notes={entityData?.notes?.relationships?.map((r) => r.entity as Post) || []} />
                    )}
                    {entityData?.deprecation?.deprecated && (
                        <DeprecationIcon
                            urn={urn}
                            deprecation={entityData?.deprecation}
                            showUndeprecate
                            refetch={refetch}
                            showText={false}
                        />
                    )}
                    {entityData?.health && <HealthIcon urn={urn} health={entityData.health} baseUrl={entityUrl} />}
                    <StructuredPropertyBadge structuredProperties={entityData?.structuredProperties} />
                    <VersioningBadge
                        versionProperties={entityData?.versionProperties ?? undefined}
                        showPopover={false}
                    />
                </NameWrapper>
                <ContextPath
                    instanceId={entityData?.dataPlatformInstance?.instanceId}
                    typeIcon={typeIcon}
                    type={displayedEntityType}
                    entityType={entityType}
                    browsePaths={entityData?.browsePathV2}
                    parentEntities={parentEntities}
                    contentRef={contentRef}
                    isContentTruncated={isContentTruncated}
                />
            </EntityDetailsContainer>
        </TitleContainer>
    );
};

export default SidebarEntityHeader;
