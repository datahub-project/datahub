import { MoreOutlined } from '@ant-design/icons';
import React, { useContext } from 'react';
import styled from 'styled-components';
import { useEntityData, useRefetch } from '../../../entity/shared/EntityContext';
import ShareMenuAction from '../../../shared/share/v2/ShareMenuAction';
import { SubscribeMenuAction } from '../../../shared/subscribe/v2/SubscribeMenuAction';
import EntitySidebarContext from '../../../sharedV2/EntitySidebarContext';
import DeleteEntityMenuItem from './DeleteEntityMenuAction';
import ExternalUrlMenuAction from './ExternalUrlMenuAction';
import MoreOptionsMenuAction from './MoreOptionsMenuAction';
import MoveEntityMenuAction from './MoveEntityMenuAction';
import RaiseIncidentMenuAction from './RaiseIncidentMenuAction';
import UpdateDeprecationMenuAction from './UpdateDeprecationMenuAction';

export enum EntityMenuItems {
    SUBSCRIBE,
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
    gap: 4px;
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

    const hasVersioningActions = !!(menuItems.has(EntityMenuItems.LINK_VERSION) || entityData?.versionProperties);

    return (
        <>
            {isClosed ? (
                <MenuItems $shouldFillAllAvailableSpace={shouldFillAllAvailableSpace}>
                    <ExternalUrlMenuAction shouldFillAllAvailableSpace={shouldFillAllAvailableSpace} />
                    {menuItems.has(EntityMenuItems.MOVE) && <MoveEntityMenuAction />}
                    {menuItems.has(EntityMenuItems.SUBSCRIBE) && <SubscribeMenuAction />}
                    {menuItems.has(EntityMenuItems.SHARE) && <ShareMenuAction />}
                    {menuItems.has(EntityMenuItems.UPDATE_DEPRECATION) && <UpdateDeprecationMenuAction />}
                    {menuItems.has(EntityMenuItems.DELETE) && (
                        <DeleteEntityMenuItem onDelete={onDelete} options={options} />
                    )}
                    {/** acryl-main only */}
                    {menuItems.has(EntityMenuItems.RAISE_INCIDENT) && <RaiseIncidentMenuAction />}
                    {hasVersioningActions && (
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
                <MenuItems $shouldFillAllAvailableSpace={shouldFillAllAvailableSpace}>
                    <ExternalUrlMenuAction shouldFillAllAvailableSpace={shouldFillAllAvailableSpace} />
                    {menuItems.has(EntityMenuItems.SUBSCRIBE) && <SubscribeMenuAction />}
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
