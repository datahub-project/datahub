import { ArrowDownOutlined, ArrowUpOutlined, MoreOutlined } from '@ant-design/icons';
import { Dropdown, Popover, Tooltip } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import FocusIcon from '../../../images/focus.svg';
import { Direction, UpdatedLineages } from '../types';
import { EntityType } from '../../../types.generated';
import ManageLineageModal from './ManageLineageModal';
import { ENTITY_TYPES_WITH_MANUAL_LINEAGE } from '../../entity/shared/constants';
import { MenuItemStyle } from '../../entity/view/menu/item/styledComponent';

const DROPDOWN_Z_INDEX = 1;
const POPOVER_Z_INDEX = 2;
const UNAUTHORIZED_TEXT = "You aren't authorized to edit lineage for this entity.";

const StyledImage = styled.img`
    width: 12px;
`;

const UnderlineWrapper = styled.span`
    text-decoration: underline;
    cursor: pointer;
`;

const MenuItemContent = styled.div``;

function PopoverContent({ centerEntity, direction }: { centerEntity?: () => void; direction: string }) {
    return (
        <div>
            <UnderlineWrapper onClick={centerEntity}>Focus</UnderlineWrapper> on this entity to make {direction} edits.
        </div>
    );
}

function getDownstreamDisabledPopoverContent(canEditLineage: boolean, isDashboard: boolean, centerEntity?: () => void) {
    if (!canEditLineage) {
        return UNAUTHORIZED_TEXT;
    }
    if (isDashboard) {
        return 'Dashboard entities have no downstream lineage';
    }
    return <PopoverContent centerEntity={centerEntity} direction="downstream" />;
}

interface Props {
    entityUrn: string;
    refetchEntity: () => void;
    setUpdatedLineages: React.Dispatch<React.SetStateAction<UpdatedLineages>>;
    disableUpstream?: boolean;
    disableDownstream?: boolean;
    centerEntity?: () => void;
    showLoading?: boolean;
    menuIcon?: React.ReactNode;
    entityType?: EntityType;
    entityPlatform?: string;
    canEditLineage?: boolean;
    disableDropdown?: boolean;
}

export default function ManageLineageMenu({
    entityUrn,
    refetchEntity,
    setUpdatedLineages,
    disableUpstream,
    disableDownstream,
    centerEntity,
    showLoading,
    menuIcon,
    entityType,
    entityPlatform,
    canEditLineage,
    disableDropdown,
}: Props) {
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [lineageDirection, setLineageDirection] = useState<Direction>(Direction.Upstream);

    function manageLineage(direction: Direction) {
        setIsModalVisible(true);
        setLineageDirection(direction);
    }

    const isCenterNode = !disableUpstream && !disableDownstream;
    const isDashboard = entityType === EntityType.Dashboard;
    const isDownstreamDisabled = disableDownstream || isDashboard || !canEditLineage;
    const isUpstreamDisabled = disableUpstream || !canEditLineage;
    const isManualLineageSupported = entityType && ENTITY_TYPES_WITH_MANUAL_LINEAGE.has(entityType);

    // if we don't show manual lineage options or the center node option, this menu has no options
    if (!isManualLineageSupported && isCenterNode) return null;

    const items = [
        isManualLineageSupported
            ? {
                  key: 0,
                  label: (
                      <MenuItemStyle onClick={() => manageLineage(Direction.Upstream)} disabled={isUpstreamDisabled}>
                          <Popover
                              content={
                                  !canEditLineage ? (
                                      UNAUTHORIZED_TEXT
                                  ) : (
                                      <PopoverContent centerEntity={centerEntity} direction="upstream" />
                                  )
                              }
                              overlayStyle={isUpstreamDisabled ? { zIndex: POPOVER_Z_INDEX } : { display: 'none' }}
                          >
                              <MenuItemContent>
                                  <ArrowUpOutlined />
                                  &nbsp; Edit Upstream
                              </MenuItemContent>
                          </Popover>
                      </MenuItemStyle>
                  ),
              }
            : null,
        isManualLineageSupported
            ? {
                  key: 1,
                  label: (
                      <MenuItemStyle
                          onClick={() => manageLineage(Direction.Downstream)}
                          disabled={isDownstreamDisabled}
                      >
                          <Popover
                              content={getDownstreamDisabledPopoverContent(!!canEditLineage, isDashboard, centerEntity)}
                              overlayStyle={isDownstreamDisabled ? { zIndex: POPOVER_Z_INDEX } : { display: 'none' }}
                          >
                              <MenuItemContent>
                                  <ArrowDownOutlined />
                                  &nbsp; Edit Downstream
                              </MenuItemContent>
                          </Popover>
                      </MenuItemStyle>
                  ),
              }
            : null,
        !isCenterNode && centerEntity
            ? {
                  key: 2,
                  label: (
                      <MenuItemStyle onClick={centerEntity}>
                          <StyledImage src={FocusIcon} alt="focus on entity" />
                          &nbsp; Focus on Entity
                      </MenuItemStyle>
                  ),
              }
            : null,
    ];

    return (
        <>
            <Tooltip title={disableDropdown ? UNAUTHORIZED_TEXT : ''}>
                <div>
                    <Dropdown
                        overlayStyle={{ zIndex: DROPDOWN_Z_INDEX }}
                        disabled={disableDropdown}
                        menu={{ items }}
                        trigger={['click']}
                    >
                        {menuIcon || <MoreOutlined style={{ fontSize: 18 }} />}
                    </Dropdown>
                </div>
            </Tooltip>
            {isModalVisible && (
                <ManageLineageModal
                    entityUrn={entityUrn}
                    lineageDirection={lineageDirection}
                    closeModal={() => setIsModalVisible(false)}
                    refetchEntity={refetchEntity}
                    setUpdatedLineages={setUpdatedLineages}
                    showLoading={showLoading}
                    entityType={entityType}
                    entityPlatform={entityPlatform}
                />
            )}
        </>
    );
}
