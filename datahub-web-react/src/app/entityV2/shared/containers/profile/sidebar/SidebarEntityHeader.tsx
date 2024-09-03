import React from 'react';
import styled from 'styled-components';
import { Container, DataPlatform, EntityType, Post } from '../../../../../../types.generated';
import { useEntityData } from '../../../../../entity/shared/EntityContext';
import HealthIcon from '../../../../../previewV2/HealthIcon';
import NotesIcon from '../../../../../previewV2/NotesIcon';
import SearchCardBrowsePath from '../../../../../previewV2/SearchCardBrowsePath';
import StaticSearchCardBrowsePath from '../../../../../previewV2/StaticSearchCardBrowsePath';
import useContentTruncation from '../../../../../shared/useContentTruncation';
import HorizontalScroller from '../../../../../sharedV2/carousel/HorizontalScroller';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { IconStyleType } from '../../../../Entity';
import EntityTitleLoadingSection from '../header/EntityHeaderLoadingSection';
import EntityName from '../header/EntityName';
import ContainerIcon from '../header/PlatformContent/ContainerIcon';
import PlatformHeaderIcons from '../header/PlatformContent/PlatformHeaderIcons';
import { getDisplayedEntityType } from '../header/utils';

const TitleContainer = styled(HorizontalScroller)`
    display: flex;
    gap: 5px;
`;

const EntityDetailsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 5px;
`;

const NameWrapper = styled.div`
    display: flex;
    gap: 8px;

    font-size: 16px;
`;

const SidebarEntityHeader = () => {
    const { entityType, entityData, loading } = useEntityData();
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

    // Determine if entity has parent containers for rendering SearchBrowsePath or StaticSearchBrowsePath
    const hasParentContainers = entityData?.parentContainers && entityData?.parentContainers.count > 0;

    const platform = entityType === EntityType.SchemaField ? entityData?.parent?.platform : entityData?.platform;
    const platforms =
        entityType === EntityType.SchemaField ? entityData?.parent?.siblingPlatforms : entityData?.siblingPlatforms;

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
                    {entityData?.health && <HealthIcon health={entityData.health} baseUrl={entityUrl} />}
                </NameWrapper>
                <div>
                    {hasParentContainers && (
                        <SearchCardBrowsePath
                            instanceId={entityData?.dataPlatformInstance?.instanceId}
                            typeIcon={typeIcon}
                            type={displayedEntityType}
                            entityType={entityType}
                            parentContainers={entityData?.parentContainers?.containers}
                            parentEntities={entityData?.parentDomains?.domains}
                            parentContainersRef={contentRef}
                            areContainersTruncated={isContentTruncated}
                        />
                    )}
                    {!hasParentContainers && (
                        <StaticSearchCardBrowsePath
                            entityType={entityType}
                            browsePaths={entityData?.browsePathV2}
                            type={displayedEntityType}
                            parentEntity={entityData?.parent}
                        />
                    )}
                </div>
            </EntityDetailsContainer>
        </TitleContainer>
    );
};

export default SidebarEntityHeader;
