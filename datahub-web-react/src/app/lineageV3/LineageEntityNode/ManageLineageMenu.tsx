import { Icon, Popover, colors } from '@components';
import { Button, Dropdown } from 'antd';
import { ItemType } from 'antd/lib/menu/hooks/useItems';
import * as QueryString from 'query-string';
import React, { Dispatch, SetStateAction, useCallback, useEffect, useState } from 'react';
import { useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { ENTITY_TYPES_WITH_MANUAL_LINEAGE } from '@app/entityV2/shared/constants';
import { LineageEntity, onClickPreventSelect } from '@app/lineageV3/common';
import { getLineageUrl } from '@app/lineageV3/lineageUtils';
import ManageLineageModal from '@app/lineageV3/manualLineage/ManageLineageModal';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, LineageDirection } from '@types';

const DROPDOWN_Z_INDEX = 100;
const POPOVER_Z_INDEX = 101;
const UNAUTHORIZED_TEXT = "You aren't authorized to edit lineage for this entity.";
const DOWNSTREAM_DISABLED_TEXT = 'Make this entity your home to make downstream edits.';
const UPSTREAM_DISABLED_TEXT = 'Make this entity your home to make upstream edits.';

const Wrapper = styled.div`
    border-radius: 4px;
    margin: 0 -4px;

    :hover {
        color: ${(p) => p.theme.styles['primary-color']};
    }
`;

const MenuItemContent = styled.div`
    display: flex;
    gap: 8px;

    font-size: 12px;
`;

const StyledButton = styled(Button)`
    height: min-content;
    padding: 0;
    border: none;
    box-shadow: none;
    transition: none;

    display: flex;
    align-items: center;
    justify-content: center;

    .ant-dropdown {
        top: 20px !important;
        left: auto !important;
        right: 0 !important;
    }
`;

const PopoverContent = styled.span`
    z-index: ${POPOVER_Z_INDEX};
`;

interface Props {
    node: LineageEntity;
    refetch: Record<LineageDirection, () => void>;
    isRootUrn: boolean;
    isGhost: boolean;
    isOpen: boolean;
    setDisplayedMenuNode: Dispatch<SetStateAction<string | null>>;
}

export default function ManageLineageMenu({ node, refetch, isRootUrn, isGhost, isOpen, setDisplayedMenuNode }: Props) {
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [lineageDirection, setLineageDirection] = useState<LineageDirection>(LineageDirection.Upstream);
    const location = useLocation();
    const history = useHistory();
    const entityRegistry = useEntityRegistry();

    // Check for lineageEditDirection URL parameter when component mounts
    useEffect(() => {
        if (isRootUrn) {
            const params = QueryString.parse(location.search);
            const editDirection = params.lineageEditDirection as string;

            if (editDirection) {
                // Convert string parameter to LineageDirection enum
                const direction =
                    editDirection.toLowerCase() === 'downstream'
                        ? LineageDirection.Downstream
                        : LineageDirection.Upstream;

                // Clear the parameter from URL
                const newParams = { ...params };
                delete newParams.lineageEditDirection;
                const newSearch = QueryString.stringify(newParams);
                history.replace({
                    pathname: location.pathname,
                    search: newSearch,
                });

                // Open the modal with the specified direction
                setLineageDirection(direction);
                setIsModalVisible(true);
            }
        }
    }, [isRootUrn, location, history]);

    function manageLineage(direction: LineageDirection) {
        setLineageDirection(direction);
        setIsModalVisible(true);
    }

    function handleMenuClick(e: React.MouseEvent<HTMLElement, MouseEvent>) {
        onClickPreventSelect(e);
        if (isOpen) {
            setDisplayedMenuNode(null);
        } else {
            setDisplayedMenuNode(node.urn);
        }
    }

    const hideMenu = useCallback(() => setIsModalVisible(false), []);

    const canEditLineage = !!node.entity?.canEditLineage;
    const disableUpstream = node.direction === LineageDirection.Downstream;
    const disableDownstream = node.direction === LineageDirection.Upstream;
    const isDashboard = node.type === EntityType.Dashboard;
    const isDownstreamDisabled = disableDownstream || isDashboard || !canEditLineage;
    const isUpstreamDisabled = disableUpstream || !canEditLineage;
    const isManualLineageSupported = ENTITY_TYPES_WITH_MANUAL_LINEAGE.has(node.type);

    const items: ItemType[] = [];

    if (!isRootUrn) {
        items.push({
            key: 'change-home-node',
            onClick: () => history.push(getLineageUrl(node.urn, node.type, location, entityRegistry)),
            label: (
                <MenuItemContent data-testid="change-home-node">
                    <Icon icon="House" source="phosphor" size="inherit" />
                    Change to Home
                </MenuItemContent>
            ),
        });
    }

    if (!isGhost && isManualLineageSupported) {
        items.push(
            {
                key: 'edit-upstream',
                disabled: isUpstreamDisabled,
                onClick: () => manageLineage(LineageDirection.Upstream),
                label: (
                    <Popover
                        content={!canEditLineage ? UNAUTHORIZED_TEXT : UPSTREAM_DISABLED_TEXT}
                        overlayStyle={isUpstreamDisabled ? { zIndex: POPOVER_Z_INDEX } : { display: 'none' }}
                    >
                        <MenuItemContent data-testid="edit-upstream-lineage">
                            <Icon icon="ArrowLeft" source="phosphor" size="inherit" />
                            Edit Upstream
                        </MenuItemContent>
                    </Popover>
                ),
            },
            {
                key: 'edit-downstream',
                disabled: isDownstreamDisabled,
                onClick: () => manageLineage(LineageDirection.Downstream),
                label: (
                    <Popover
                        placement="bottom"
                        content={getDownstreamDisabledPopoverContent(canEditLineage, isDashboard)}
                        overlayStyle={!isDownstreamDisabled ? { display: 'none' } : undefined}
                    >
                        <MenuItemContent data-testid="edit-downstream-lineage">
                            <Icon icon="ArrowRight" source="phosphor" size="inherit" />
                            Edit Downstream
                        </MenuItemContent>
                    </Popover>
                ),
            },
        );
    }

    items.push({
        key: 'copy-urn',
        onClick: () => navigator.clipboard.writeText(node.urn),
        label: (
            <MenuItemContent data-testid="change-home-node">
                <Icon icon="Copy" source="phosphor" size="inherit" />
                Copy Urn
            </MenuItemContent>
        ),
    });

    if (!items.length) return null;
    return (
        <Wrapper>
            <StyledButton onClick={handleMenuClick} type="text" data-testid={`manage-lineage-menu-${node.urn}`}>
                <Dropdown
                    open={isOpen}
                    overlayStyle={{ zIndex: DROPDOWN_Z_INDEX }}
                    placement="topRight"
                    menu={{ items, style: { boxShadow: 'initial', border: `1px solid ${colors.gray[100]}` } }}
                >
                    <Icon icon="DotsThreeVertical" source="phosphor" color="gray" />
                </Dropdown>
            </StyledButton>
            {isModalVisible && (
                <ManageLineageModal
                    node={node}
                    direction={lineageDirection}
                    closeModal={hideMenu}
                    refetch={refetch[lineageDirection]}
                />
            )}
        </Wrapper>
    );
}

function getDownstreamDisabledPopoverContent(canEditLineage: boolean, isDashboard: boolean) {
    let text = '';
    if (!canEditLineage) {
        text = UNAUTHORIZED_TEXT;
    } else if (isDashboard) {
        text = 'Dashboard entities have no downstream lineage';
    } else {
        text = DOWNSTREAM_DISABLED_TEXT;
    }
    return <PopoverContent>{text}</PopoverContent>;
}
