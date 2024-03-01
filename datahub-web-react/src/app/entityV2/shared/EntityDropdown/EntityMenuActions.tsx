import React, { useContext } from 'react';
import styled from 'styled-components';
import { MoreOutlined } from '@ant-design/icons';
import ExternalUrlMenuAction from './ExternalUrlMenuAction';
import ShareMenuAction from '../../../shared/share/v2/ShareMenuAction';
import MoveEntityMenuAction from './MoveEntityMenuAction';
import UpdateDeprecationMenuAction from './UpdateDeprecationMenuAction';
import RaiseIncidentMenuAction from './RaiseIncidentMenuAction';
import DeleteEntityMenuItem from './DeleteEntityMenuAction';
import MoreOptionsMenuAction from './MoreOptionsMenuAction';
import { SubscribeMenuAction } from '../../../shared/subscribe/v2/SubscribeMenuAction';
import EntitySidebarContext from '../../../shared/EntitySidebarContext';
import { useEntityData, useRefetch } from '../EntityContext';

export enum EntityMenuItems {
    EXTERNAL_URL,
    SUBSCRIBE,
    SHARE,
    COPY_URL,
    UPDATE_DEPRECATION,
    ADD_TERM, // Make primary
    ADD_TERM_GROUP, // Make primary
    MOVE,
    DELETE,
    // acryl-main only
    RAISE_INCIDENT,
}

export const MenuIcon = styled(MoreOutlined)<{ fontSize?: number }>`
    display: flex;
    justify-content: center;
    align-items: center;
    font-size: ${(props) => props.fontSize || '24'}px;
    height: 32px;
    margin-left: 5px;
`;

const MenuItems = styled.div`
    display: flex;
    align-items: center;
    justify-content: end;
`;

export interface Options {
    hideDeleteMessage?: boolean;
    skipDeleteWait?: boolean;
}

interface Props {
    menuItems: Set<EntityMenuItems>;
    options?: Options;
    onDeleteEntity?: () => void;
}

function EntityMenuActions(props: Props) {
    const { menuItems, onDeleteEntity: onDelete, options } = props;

    const { isClosed } = useContext(EntitySidebarContext);

    const { urn, entityType, entityData } = useEntityData();

    const refetch = useRefetch();

    return (
        <>
            {isClosed ? (
                <MenuItems>
                    {menuItems.has(EntityMenuItems.EXTERNAL_URL) && <ExternalUrlMenuAction />}
                    {menuItems.has(EntityMenuItems.MOVE) && <MoveEntityMenuAction />}
                    {menuItems.has(EntityMenuItems.SUBSCRIBE) && <SubscribeMenuAction entityUrn={urn} />}
                    {menuItems.has(EntityMenuItems.SHARE) && <ShareMenuAction />}
                    {menuItems.has(EntityMenuItems.UPDATE_DEPRECATION) && <UpdateDeprecationMenuAction />}
                    {menuItems.has(EntityMenuItems.DELETE) && (
                        <DeleteEntityMenuItem onDelete={onDelete} options={options} />
                    )}
                    {/** acryl-main only */}
                    {menuItems.has(EntityMenuItems.RAISE_INCIDENT) && <RaiseIncidentMenuAction />}{' '}
                </MenuItems>
            ) : (
                <MenuItems>
                    {menuItems.has(EntityMenuItems.EXTERNAL_URL) && <ExternalUrlMenuAction />}
                    <MoreOptionsMenuAction
                        menuItems={menuItems}
                        urn={urn}
                        entityType={entityType}
                        entityData={entityData}
                        refetch={refetch}
                    />
                </MenuItems>
            )}
        </>
    );
}

export default EntityMenuActions;
