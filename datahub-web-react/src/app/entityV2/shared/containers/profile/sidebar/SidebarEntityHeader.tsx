import React from 'react';
import styled from 'styled-components';
import { useEntityData, useRefetch } from '../../../../../entity/shared/EntityContext';
import { Container, DataPlatform, EntityType, Post, Entity } from '../../../../../../types.generated';
import ContextPath from '../../../../../previewV2/ContextPath';
import HealthIcon from '../../../../../previewV2/HealthIcon';
import NotesIcon from '../../../../../previewV2/NotesIcon';
import useContentTruncation from '../../../../../shared/useContentTruncation';
import HorizontalScroller from '../../../../../sharedV2/carousel/HorizontalScroller';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { IconStyleType } from '../../../../Entity';
import EntityTitleLoadingSection from '../header/EntityHeaderLoadingSection';
import EntityName from '../header/EntityName';
import ContainerIcon from '../header/PlatformContent/ContainerIcon';
import PlatformHeaderIcons from '../header/PlatformContent/PlatformHeaderIcons';
import StructuredPropertyBadge from '../header/StructuredPropertyBadge';
import { getDisplayedEntityType } from '../header/utils';
import { DeprecationIcon } from '../../../components/styled/DeprecationIcon';

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
