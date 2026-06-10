import { ArrowLeftOutlined, ArrowRightOutlined, MoreOutlined } from '@ant-design/icons';
import { Popover } from '@components';
import { Button, Dropdown, Menu } from 'antd';
import * as QueryString from 'query-string';
import React, { useCallback, useContext, useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { ENTITY_TYPES_WITH_MANUAL_LINEAGE } from '@app/entityV2/shared/constants';
import { LineageDisplayContext, LineageEntity, onClickPreventSelect } from '@app/lineageV2/common';
import ManageLineageModal from '@app/lineageV2/manualLineage/ManageLineageModal';

import { EntityType, LineageDirection } from '@types';

const DROPDOWN_Z_INDEX = 100;
const POPOVER_Z_INDEX = 101;

const Wrapper = styled.div`
    border-radius: 4px;
    position: absolute;
    right: 3px;
    top: 8px;

    :hover {
        color: ${(p) => p.theme.colors.textHover};
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
    isRootUrn: boolean;
}

export default function ManageLineageMenu({ node, refetch, isRootUrn }: Props) {
    const { t } = useTranslation('lineage');
    const { displayedMenuNode, setDisplayedMenuNode } = useContext(LineageDisplayContext);
    const isMenuVisible = displayedMenuNode === node.urn;
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [lineageDirection, setLineageDirection] = useState<LineageDirection>(LineageDirection.Upstream);
    const location = useLocation();
    const history = useHistory();

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
            <StyledButton onClick={handleMenuClick} type="text" data-testid={`manage-lineage-menu-${node.urn}`}>
                <Dropdown
                    open={isMenuVisible}
                    overlayStyle={{ zIndex: DROPDOWN_Z_INDEX }}
                    getPopupContainer={(triggerNode) => triggerNode.parentElement || triggerNode}
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
                                            content={
                                                !canEditLineage
                                                    ? t('manageLineage.unauthorized')
                                                    : t('manageLineage.upstreamDisabled')
                                            }
                                            overlayStyle={
                                                isUpstreamDisabled ? { zIndex: POPOVER_Z_INDEX } : { display: 'none' }
                                            }
                                        >
                                            <MenuItemContent data-testid="edit-upstream-lineage">
                                                <ArrowLeftOutlined />
                                                &nbsp; {t('manageLineage.editUpstream')}
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
                                            content={getDownstreamDisabledPopoverContent(
                                                !!canEditLineage,
                                                isDashboard,
                                                t,
                                            )}
                                            overlayStyle={!isDownstreamDisabled ? { display: 'none' } : undefined}
                                        >
                                            <MenuItemContent data-testid="edit-downstream-lineage">
                                                <ArrowRightOutlined />
                                                &nbsp; {t('manageLineage.editDownstream')}
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

function getDownstreamDisabledPopoverContent(
    canEditLineage: boolean,
    isDashboard: boolean,
    t: (key: string) => string,
) {
    let text = '';
    if (!canEditLineage) {
        text = t('manageLineage.unauthorized');
    } else if (isDashboard) {
        text = t('manageLineage.dashboardNoDownstream');
    } else {
        text = t('manageLineage.downstreamDisabled');
    }
    return <PopoverContent>{text}</PopoverContent>;
}
