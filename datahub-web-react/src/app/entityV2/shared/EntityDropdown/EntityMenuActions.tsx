import { MoreOutlined } from '@ant-design/icons';
import React, { useContext } from 'react';
import styled from 'styled-components';
import { useEntityData, useRefetch } from '../../../entity/shared/EntityContext';
import { ENTITY_PROFILE_V2_SUBSCRIPTION_ID } from '../../../onboarding/configV2/EntityProfileOnboardingConfig';
import ShareMenuAction from '../../../shared/share/v2/ShareMenuAction';
import EntitySidebarContext from '../../../sharedV2/EntitySidebarContext';
import DeleteEntityMenuItem from './DeleteEntityMenuAction';
import ExternalUrlMenuAction from './ExternalUrlMenuAction';
import MoreOptionsMenuAction from './MoreOptionsMenuAction';
import MoveEntityMenuAction from './MoveEntityMenuAction';
import RaiseIncidentMenuAction from './RaiseIncidentMenuAction';
import UpdateDeprecationMenuAction from './UpdateDeprecationMenuAction';

export enum EntityMenuItems {
    EXTERNAL_URL,
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

const MoreOptionsContainer = styled.div``;

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
                    {menuItems.has(EntityMenuItems.SHARE) && <ShareMenuAction />}
                    {menuItems.has(EntityMenuItems.UPDATE_DEPRECATION) && <UpdateDeprecationMenuAction />}
                    {menuItems.has(EntityMenuItems.DELETE) && (
                        <DeleteEntityMenuItem onDelete={onDelete} options={options} />
                    )}
                    {menuItems.has(EntityMenuItems.RAISE_INCIDENT) && <RaiseIncidentMenuAction />}{' '}
                </MenuItems>
            ) : (
                <MenuItems>
                    {menuItems.has(EntityMenuItems.EXTERNAL_URL) && <ExternalUrlMenuAction />}
                    <MoreOptionsContainer id={ENTITY_PROFILE_V2_SUBSCRIPTION_ID}>
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