import { ArrowDownOutlined, ArrowUpOutlined, MoreOutlined } from '@ant-design/icons';
import { Popover, Tooltip } from '@components';
import { Direction } from '@src/app/lineage/types';
import { Dropdown } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { EntityType } from '../../../../../types.generated';
import { ENTITY_TYPES_WITH_MANUAL_LINEAGE } from '../../../../entity/shared/constants';
import { MenuItemStyle } from '../../../../entity/view/menu/item/styledComponent';

const DROPDOWN_Z_INDEX = 100;
const POPOVER_Z_INDEX = 101;
const UNAUTHORIZED_TEXT = "You aren't authorized to edit lineage for this entity.";

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
    disableUpstream?: boolean;
    disableDownstream?: boolean;
    centerEntity?: () => void;
    menuIcon?: React.ReactNode;
    entityType?: EntityType;
    canEditLineage?: boolean;
    disableDropdown?: boolean;
    setVisualizeViewInEditMode: (view: boolean, direction: Direction) => void;
}

export default function ManageLineageMenuForImpactAnalysis({
    disableUpstream,
    disableDownstream,
    centerEntity,
    menuIcon,
    entityType,
    canEditLineage,
    disableDropdown,
    setVisualizeViewInEditMode,
}: Props) {
    function manageLineage(direction: Direction) {
        setVisualizeViewInEditMode(true, direction);
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
                              <MenuItemContent data-testid="edit-upstream-lineage">
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
                              <MenuItemContent data-testid="edit-downstream-lineage">
                                  <ArrowDownOutlined />
                                  &nbsp; Edit Downstream
                              </MenuItemContent>
                          </Popover>
                      </MenuItemStyle>
                  ),
              }
            : null,
    ];

    return (
        <>
            <Tooltip title={disableDropdown ? UNAUTHORIZED_TEXT : ''}>
                <div data-testid="lineage-edit-menu-button">
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
        </>
    );
}
