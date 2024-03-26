import React from 'react';
import styled from 'styled-components';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { useEntityData } from '../../../EntityContext';
import { Container, DataPlatform, EntityType } from '../../../../../../types.generated';
import EntityTitleLoadingSection from '../header/EntityHeaderLoadingSection';
import PlatformHeaderIcons from '../header/PlatformContent/PlatformHeaderIcons';
import EntityName from '../header/EntityName';
import SearchCardBrowsePath from '../../../../../previewV2/SearchCardBrowsePath';
import { getDisplayedEntityType } from '../header/utils';
import useContentTruncation from '../../../../../shared/useContentTruncation';
import ContainerIcon from '../header/PlatformContent/ContainerIcon';
import { IconStyleType } from '../../../../Entity';
import HealthIcon from '../../../../../previewV2/HealthIcon';
import { isUnhealthy } from '../../../../../shared/health/healthUtils';

const Wrapper = styled.div`
    display: flex;
    border-bottom: 1px dashed;
    border-bottom-color: currentcolor;
    border-color: rgba(0, 0, 0, 0.3);
`;

const TitleContainer = styled.div`
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
`;

const SidebarEntityHeader = () => {
    const { entityType, entityData, loading } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const entityLogoComponent = entityRegistry.getIcon(entityType, 24, IconStyleType.ACCENT);
    const entityUrl = entityRegistry.getEntityUrl(entityType, entityData?.urn as string);

    const { contentRef, isContentTruncated } = useContentTruncation(entityData);
    const typeIcon =
        entityType === EntityType.Container ? (
            <ContainerIcon container={entityData as Container} />
        ) : (
            entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT)
        );
    const displayedEntityType = getDisplayedEntityType(entityData, entityRegistry, entityType);

    return (
        <Wrapper>
            {(loading && <EntityTitleLoadingSection />) || (
                <TitleContainer>
                    <PlatformHeaderIcons
                        platform={entityData?.platform as DataPlatform}
                        entityLogoComponent={entityLogoComponent}
                        platforms={entityData?.siblingPlatforms as DataPlatform[]}
                        size={24}
                    />
                    <EntityDetailsContainer>
                        <NameWrapper>
                            <EntityName isNameEditable={false} />
                            {entityData?.health && isUnhealthy(entityData.health) && (
                                <HealthIcon health={entityData.health} baseUrl={entityUrl} />
                            )}
                        </NameWrapper>
                        <div>
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
                        </div>
                    </EntityDetailsContainer>
                </TitleContainer>
            )}
        </Wrapper>
    );
};

export default SidebarEntityHeader;
