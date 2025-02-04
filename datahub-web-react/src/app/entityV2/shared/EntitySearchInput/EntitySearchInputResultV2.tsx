import { getDisplayedEntityType } from '@app/entityV2/shared/containers/profile/header/utils';
import ContextPath from '@app/previewV2/ContextPath';
import useContentTruncation from '@app/shared/useContentTruncation';
import { Skeleton } from 'antd';
import React from 'react';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Text } from '@components';
import { Entity } from '@types';
import styled from 'styled-components';

const Wrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 10px;
`;

const TextWrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

const IconContainer = styled.img`
    height: 24px;
    min-width: 24px;
`;

type Props = {
    entity: Entity;
};

export default function EntitySearchInputResultV2({ entity }: Props) {
    const entityRegistry = useEntityRegistry();
    const properties = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const platformIcon = properties?.platform?.properties?.logoUrl;

    const { contentRef, isContentTruncated } = useContentTruncation(properties);
    const displayedEntityType = getDisplayedEntityType(properties, entityRegistry, entity.type);

    return (
        <Wrapper>
            {!platformIcon && <Skeleton.Avatar size={24} active />}
            {platformIcon && <IconContainer src={platformIcon} />}
            <TextWrapper>
                <Text size="lg">{entityRegistry.getDisplayName(entity.type, entity)}</Text>
                <ContextPath
                    instanceId={properties?.dataPlatformInstance?.instanceId}
                    entityType={entity.type}
                    type={displayedEntityType}
                    browsePaths={properties?.browsePathV2}
                    parentEntities={
                        properties?.parentContainers?.containers ||
                        properties?.parentDomains?.domains ||
                        properties?.parentNodes?.nodes
                    }
                    contentRef={contentRef}
                    isContentTruncated={isContentTruncated}
                    linksDisabled
                />
            </TextWrapper>
        </Wrapper>
    );
}
