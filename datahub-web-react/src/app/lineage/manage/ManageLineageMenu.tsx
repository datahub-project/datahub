import { ArrowDownOutlined, ArrowUpOutlined, MoreOutlined } from '@ant-design/icons';
import { Dropdown, Popover, Tooltip } from 'antd';
import i18next from 'i18next';
import React, { useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ENTITY_TYPES_WITH_MANUAL_LINEAGE } from '@app/entity/shared/constants';
import { MenuItemStyle } from '@app/entity/view/menu/item/styledComponent';
import ManageLineageModal from '@app/lineage/manage/ManageLineageModal';
import { Direction, UpdatedLineages } from '@app/lineage/types';

import { EntityType } from '@types';

import FocusIcon from '@images/focus.svg';

const DROPDOWN_Z_INDEX = 1;
const POPOVER_Z_INDEX = 2;

const StyledImage = styled.img`
    width: 12px;
`;

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

function getDownstreamDisabledPopoverContent(canEditLineage: boolean, isDashboard: boolean, centerEntity?: () => void) {
    if (!canEditLineage) {
        return i18next.t('lineage:manageLineage.unauthorized');
    }
    if (isDashboard) {
        return i18next.t('lineage:manageLineage.dashboardNoDownstream');
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
    const { t } = useTranslation('lineage');
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
                                      t('manageLineage.unauthorized')
                                  ) : (
                                      <PopoverContent centerEntity={centerEntity} direction="upstream" />
                                  )
                              }
                              overlayStyle={isUpstreamDisabled ? { zIndex: POPOVER_Z_INDEX } : { display: 'none' }}
                          >
                              <MenuItemContent>
                                  <ArrowUpOutlined />
                                  &nbsp; {t('manageLineage.editUpstream')}
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
                                  &nbsp; {t('manageLineage.editDownstream')}
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
                          <StyledImage src={FocusIcon} alt={t('manageLineage.focusOnEntityAlt')} />
                          &nbsp; {t('manageLineage.focusOnEntity')}
                      </MenuItemStyle>
                  ),
              }
            : null,
    ];

    return (
        <>
            <Tooltip title={disableDropdown ? t('manageLineage.unauthorized') : ''}>
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
