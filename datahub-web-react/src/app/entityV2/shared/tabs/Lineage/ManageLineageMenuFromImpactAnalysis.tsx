import { ArrowDownOutlined, ArrowUpOutlined, MoreOutlined } from '@ant-design/icons';
import { Popover, Tooltip } from '@components';
import { Dropdown } from 'antd';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ENTITY_TYPES_WITH_MANUAL_LINEAGE } from '@app/entity/shared/constants';
import { MenuItemStyle } from '@app/entity/view/menu/item/styledComponent';
import { Direction } from '@src/app/lineage/types';

import { EntityType } from '@types';

const DROPDOWN_Z_INDEX = 100;
const POPOVER_Z_INDEX = 101;

const UnderlineWrapper = styled.span`
    text-decoration: underline;
    cursor: pointer;
`;

const MenuItemContent = styled.div``;

function PopoverContent({
    centerEntity,
    direction,
}: {
    centerEntity?: () => void;
    direction: 'upstream' | 'downstream';
}) {
    return (
        <div>
            <Trans
                i18nKey={
                    direction === 'upstream'
                        ? 'lineage:manageLineage.focusPopoverContentUpstream'
                        : 'lineage:manageLineage.focusPopoverContentDownstream'
                }
                components={{ focus: <UnderlineWrapper onClick={centerEntity} /> }}
            />
        </div>
    );
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
    const { t } = useTranslation('lineage');

    function manageLineage(direction: Direction) {
        setVisualizeViewInEditMode(true, direction);
    }

    const isCenterNode = !disableUpstream && !disableDownstream;
    const isDashboard = entityType === EntityType.Dashboard;
    const isDownstreamDisabled = disableDownstream || isDashboard || !canEditLineage;
    const isUpstreamDisabled = disableUpstream || !canEditLineage;
    const isManualLineageSupported = entityType && ENTITY_TYPES_WITH_MANUAL_LINEAGE.has(entityType);

    const unauthorizedText = t('manageLineage.unauthorized');

    function getDownstreamDisabledPopoverContent() {
        if (!canEditLineage) {
            return unauthorizedText;
        }
        if (isDashboard) {
            return t('manageLineage.dashboardNoDownstream');
        }
        return <PopoverContent centerEntity={centerEntity} direction="downstream" />;
    }

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
                                      unauthorizedText
                                  ) : (
                                      <PopoverContent centerEntity={centerEntity} direction="upstream" />
                                  )
                              }
                              overlayStyle={isUpstreamDisabled ? { zIndex: POPOVER_Z_INDEX } : { display: 'none' }}
                          >
                              <MenuItemContent data-testid="edit-upstream-lineage">
                                  <ArrowUpOutlined />
                                  &nbsp;{t('manageLineage.editUpstream')}
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
                              content={getDownstreamDisabledPopoverContent()}
                              overlayStyle={isDownstreamDisabled ? { zIndex: POPOVER_Z_INDEX } : { display: 'none' }}
                          >
                              <MenuItemContent data-testid="edit-downstream-lineage">
                                  <ArrowDownOutlined />
                                  &nbsp;{t('manageLineage.editDownstream')}
                              </MenuItemContent>
                          </Popover>
                      </MenuItemStyle>
                  ),
              }
            : null,
    ];

    return (
        <>
            <Tooltip title={disableDropdown ? unauthorizedText : ''}>
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
