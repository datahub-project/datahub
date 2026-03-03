import { MoreOutlined } from '@ant-design/icons';
import React, { useContext } from 'react';
import styled from 'styled-components';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import AnnounceMenuAction from '@app/entityV2/shared/EntityDropdown/AnnounceMenuAction';
import DeleteEntityMenuItem from '@app/entityV2/shared/EntityDropdown/DeleteEntityMenuAction';
import ExternalUrlMenuAction from '@app/entityV2/shared/EntityDropdown/ExternalUrlMenuAction';
import MoreOptionsMenuAction from '@app/entityV2/shared/EntityDropdown/MoreOptionsMenuAction';
import MoveEntityMenuAction from '@app/entityV2/shared/EntityDropdown/MoveEntityMenuAction';
import RaiseIncidentMenuAction from '@app/entityV2/shared/EntityDropdown/RaiseIncidentMenuAction';
import UpdateDeprecationMenuAction from '@app/entityV2/shared/EntityDropdown/UpdateDeprecationMenuAction';
import ShareMenuAction from '@app/shared/share/v2/ShareMenuAction';
import EntitySidebarContext from '@app/sharedV2/EntitySidebarContext';
import { useAppConfig } from '@src/app/useAppConfig';

export enum EntityMenuItems {
    SHARE,
    COPY_URL,
    UPDATE_DEPRECATION,
    ADD_TERM, // Make primary
    ADD_TERM_GROUP, // Make primary
    MOVE,
    DELETE, // acryl-main only
    EDIT, // acryl-main only
    ANNOUNCE, // acryl-main only
    RAISE_INCIDENT,
    LINK_VERSION,
    CLONE,
}

export const MenuIcon = styled(MoreOutlined)<{ fontSize?: number }>`
    display: flex;
    justify-content: center;
    align-items: center;
    font-size: ${(props) => props.fontSize || '24'}px;
    height: 32px;
    margin-left: 5px;
`;

const MenuItems = styled.div<{ $shouldFillAllAvailableSpace?: boolean }>`
    display: flex;
    gap: 8px;
    align-items: center;
    justify-content: end;
    ${(props) => props.$shouldFillAllAvailableSpace && 'width: 100%;'}
`;

const MoreOptionsContainer = styled.div``;

export interface Options {
    hideDeleteMessage?: boolean;
    skipDeleteWait?: boolean;
}

interface Props {
    menuItems: Set<EntityMenuItems>;
    options?: Options;
    onDeleteEntity?: () => void;
    shouldExternalLinksFillAllAvailableSpace?: boolean;
}

function EntityMenuActions(props: Props) {
    const { menuItems, onDeleteEntity: onDelete, options, shouldExternalLinksFillAllAvailableSpace } = props;

    const { isClosed } = useContext(EntitySidebarContext);

    const { urn, entityType, entityData } = useEntityData();

    const refetch = useRefetch();

    const shouldFillAllAvailableSpace = shouldExternalLinksFillAllAvailableSpace;

    const { entityVersioningEnabled } = useAppConfig().config.featureFlags;

    const hasVersioningActions = !!(menuItems.has(EntityMenuItems.LINK_VERSION) || entityData?.versionProperties);

    return (
        <>
            {isClosed ? (
                <MenuItems $shouldFillAllAvailableSpace={shouldFillAllAvailableSpace} data-testid="entity-menu-actions">
                    <ExternalUrlMenuAction shouldFillAllAvailableSpace={shouldFillAllAvailableSpace} />
                    {menuItems.has(EntityMenuItems.MOVE) && <MoveEntityMenuAction />}
                    {menuItems.has(EntityMenuItems.ANNOUNCE) && <AnnounceMenuAction />}
                    {menuItems.has(EntityMenuItems.SHARE) && <ShareMenuAction />}
                    {menuItems.has(EntityMenuItems.UPDATE_DEPRECATION) && <UpdateDeprecationMenuAction />}
                    {menuItems.has(EntityMenuItems.DELETE) && (
                        <DeleteEntityMenuItem onDelete={onDelete} options={options} />
                    )}
                    {menuItems.has(EntityMenuItems.RAISE_INCIDENT) && <RaiseIncidentMenuAction />}
                    {entityVersioningEnabled && hasVersioningActions && (
                        <MoreOptionsContainer>
                            <MoreOptionsMenuAction
                                menuItems={
                                    menuItems.has(EntityMenuItems.LINK_VERSION)
                                        ? new Set([EntityMenuItems.LINK_VERSION])
                                        : new Set()
                                }
                                urn={urn}
                                entityType={entityType}
                                entityData={entityData}
                                refetch={refetch}
                            />
                        </MoreOptionsContainer>
                    )}
                </MenuItems>
            ) : (
                <MenuItems $shouldFillAllAvailableSpace={shouldFillAllAvailableSpace} data-testid="entity-menu-actions">
                    <ExternalUrlMenuAction shouldFillAllAvailableSpace={shouldFillAllAvailableSpace} />
                    <MoreOptionsContainer>
                        <MoreOptionsMenuAction
                            menuItems={menuItems}
                            urn={urn}
                            entityType={entityType}
                            entityData={entityData}
                            refetch={refetch}
                        />
                    </MoreOptionsContainer>
                </MenuItems>
            )}
        </>
    );
}

export default EntityMenuActions;
