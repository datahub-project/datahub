import { ArrowLeftOutlined, ArrowRightOutlined, MoreOutlined } from '@ant-design/icons';
import Colors from '@components/theme/foundations/colors';
import { Button, Dropdown, Menu } from 'antd';
import { Popover } from '@components';
import styled from 'styled-components';
import React, { useCallback, useContext, useState } from 'react';
import { EntityType, LineageDirection } from '../../../types.generated';
import ManageLineageModal from '../manualLineage/ManageLineageModal';
import { LineageDisplayContext, LineageEntity, onClickPreventSelect } from '../common';
import { ENTITY_TYPES_WITH_MANUAL_LINEAGE } from '../../entityV2/shared/constants';

const DROPDOWN_Z_INDEX = 100;
const POPOVER_Z_INDEX = 101;
const UNAUTHORIZED_TEXT = "You aren't authorized to edit lineage for this entity.";
const DOWNSTREAM_DISABLED_TEXT = 'Make this entity your home to make downstream edits.';
const UPSTREAM_DISABLED_TEXT = 'Make this entity your home to make upstream edits.';

const Wrapper = styled.div`
    border-radius: 4px;
    position: absolute;
    right: 3px;
    top: 8px;

    :hover {
        color: ${Colors.violet[500]};
    }
`;

const StyledIcon = styled(MoreOutlined)`
    background: transparent;
`;

const StyledMenuItem = styled(Menu.Item)`
    padding: 0;
`;

const MenuItemContent = styled.div`
    padding: 5px 12px;
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
}

export default function ManageLineageMenu({ node, refetch }: Props) {
    const { displayedMenuNode, setDisplayedMenuNode } = useContext(LineageDisplayContext);
    const isMenuVisible = displayedMenuNode === node.urn;
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [lineageDirection, setLineageDirection] = useState<LineageDirection>(LineageDirection.Upstream);

    function manageLineage(direction: LineageDirection) {
        setLineageDirection(direction);
        setIsModalVisible(true);
    }

    function handleMenuClick(e: React.MouseEvent<HTMLElement, MouseEvent>) {
        onClickPreventSelect(e);
        if (isMenuVisible) {
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

    if (!isManualLineageSupported) return null;

    return (
        <Wrapper>
            <StyledButton onClick={handleMenuClick} type="text">
                <Dropdown
                    open={isMenuVisible}
                    overlayStyle={{ zIndex: DROPDOWN_Z_INDEX }}
                    getPopupContainer={(t) => t.parentElement || t}
                    overlay={
                        <Menu>
                            {isManualLineageSupported && (
                                <>
                                    <StyledMenuItem
                                        key="0"
                                        onClick={() => manageLineage(LineageDirection.Upstream)}
                                        disabled={isUpstreamDisabled}
                                    >
                                        <Popover
                                            content={!canEditLineage ? UNAUTHORIZED_TEXT : UPSTREAM_DISABLED_TEXT}
                                            overlayStyle={
                                                isUpstreamDisabled ? { zIndex: POPOVER_Z_INDEX } : { display: 'none' }
                                            }
                                        >
                                            <MenuItemContent>
                                                <ArrowLeftOutlined />
                                                &nbsp; Edit Upstream
                                            </MenuItemContent>
                                        </Popover>
                                    </StyledMenuItem>
                                    <StyledMenuItem
                                        key="1"
                                        onClick={() => manageLineage(LineageDirection.Downstream)}
                                        disabled={isDownstreamDisabled}
                                    >
                                        <Popover
                                            placement="bottom"
                                            content={getDownstreamDisabledPopoverContent(!!canEditLineage, isDashboard)}
                                            overlayStyle={!isDownstreamDisabled ? { display: 'none' } : undefined}
                                        >
                                            <MenuItemContent>
                                                <ArrowRightOutlined />
                                                &nbsp; Edit Downstream
                                            </MenuItemContent>
                                        </Popover>
                                    </StyledMenuItem>
                                </>
                            )}
                        </Menu>
                    }
                >
                    <StyledIcon style={{ fontSize: 'inherit' }} />
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
