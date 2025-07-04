import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { StyledRightOutlined } from '@app/entity/shared/containers/profile/header/PlatformContent/ParentNodesView';
import { getPlatformName } from '@app/entity/shared/utils';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity } from '@types';

const EntityWrapper = styled.div<{ shrinkPadding?: boolean }>`
    border-bottom: 1px solid ${ANTD_GRAY[4]};
    padding: ${(props) => (props.shrinkPadding ? '4px 6px' : '12px 20px')};
`;

const PlatformContent = styled.div<{ removeMargin?: boolean }>`
    display: flex;
    align-items: center;
    font-size: 10px;
    color: ${ANTD_GRAY[7]};
    margin-bottom: ${(props) => (props.removeMargin ? '0' : '5px')};
`;

const StyledDivider = styled(Divider)`
    margin: 0 5px;
`;

const PlatformLogo = styled.img`
    height: 14px;
    margin-right: 5px;
`;

export const EntityName = styled.span<{ shrinkSize?: boolean }>`
    font-size: ${(props) => (props.shrinkSize ? '12px' : '14px')};
    font-weight: bold;
`;

interface Props {
    entity: Entity;
    displaySearchResult?: boolean;
}

export default function LineageEntityView({ entity, displaySearchResult }: Props) {
    const entityRegistry = useEntityRegistry();
    const genericProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const { entityData } = useEntityData();

    const platformLogoUrl = genericProps?.platform?.properties?.logoUrl;
    const platformName = getPlatformName(genericProps);
    const containers = entityData?.parentContainers?.containers;
    const remainingContainers = containers?.slice(1);
    const directContainer = containers ? containers[0] : null;

    return (
        <EntityWrapper shrinkPadding={displaySearchResult}>
            <PlatformContent removeMargin={displaySearchResult}>
                <span>
                    {capitalizeFirstLetterOnly(genericProps?.subTypes?.typeNames?.[0]) ||
                        entityRegistry.getEntityName(entity.type)}
                </span>
                {platformName && <StyledDivider type="vertical" data-testid="divider" />}
                {platformLogoUrl && (
                    <PlatformLogo src={platformLogoUrl} alt="platform logo" data-testid="platform-logo" />
                )}
                <span>{platformName}</span>
                {remainingContainers &&
                    remainingContainers.map((container) => (
                        <>
                            <StyledRightOutlined data-testid="right-arrow" />
                            <span>{container?.properties?.name}</span>
                        </>
                    ))}
                {directContainer && (
                    <>
                        <StyledRightOutlined data-testid="right-arrow" />
                        <span>{directContainer?.properties?.name}</span>
                    </>
                )}
            </PlatformContent>
            <EntityName shrinkSize={displaySearchResult}>
                {entityRegistry.getDisplayName(entity.type, entity)}
            </EntityName>
        </EntityWrapper>
    );
}
