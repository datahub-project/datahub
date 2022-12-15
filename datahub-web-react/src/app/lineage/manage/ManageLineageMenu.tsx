import { ArrowDownOutlined, ArrowUpOutlined, MoreOutlined } from '@ant-design/icons';
import { Dropdown, Menu, Popover } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import FocusIcon from '../../../images/focus.svg';
import { Direction, UpdatedLineages } from '../types';
import { EntityType } from '../../../types.generated';
import ManageLineageModal from './ManageLineageModal';

const DROPDOWN_Z_INDEX = 1;
const POPOVER_Z_INDEX = 2;

const StyledImage = styled.img`
    width: 12px;
`;

const UnderlineWrapper = styled.span`
    text-decoration: underline;
    cursor: pointer;
`;

function PopoverContent({ centerEntity, direction }: { centerEntity?: () => void; direction: string }) {
    return (
        <div>
            <UnderlineWrapper onClick={centerEntity}>Focus</UnderlineWrapper> on this entity to make {direction} edits.
        </div>
    );
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
}: Props) {
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [lineageDirection, setLineageDirection] = useState<Direction>(Direction.Upstream);

    function manageLineage(direction: Direction) {
        setIsModalVisible(true);
        setLineageDirection(direction);
    }

    const isCenterNode = disableUpstream || disableDownstream;

    return (
        <>
            <Dropdown
                overlayStyle={{ zIndex: DROPDOWN_Z_INDEX }}
                overlay={
                    <Menu>
                        <Popover
                            content={<PopoverContent centerEntity={centerEntity} direction="upstream" />}
                            overlayStyle={disableUpstream ? { zIndex: POPOVER_Z_INDEX } : { display: 'none' }}
                        >
                            <div>
                                <Menu.Item
                                    key="0"
                                    onClick={() => manageLineage(Direction.Upstream)}
                                    disabled={disableUpstream}
                                >
                                    <ArrowUpOutlined />
                                    &nbsp; Edit Upstream
                                </Menu.Item>
                            </div>
                        </Popover>
                        <Popover
                            content={<PopoverContent centerEntity={centerEntity} direction="downstream" />}
                            overlayStyle={disableDownstream ? { zIndex: POPOVER_Z_INDEX } : { display: 'none' }}
                        >
                            <div>
                                <Menu.Item
                                    key="1"
                                    onClick={() => manageLineage(Direction.Downstream)}
                                    disabled={disableDownstream}
                                >
                                    <ArrowDownOutlined />
                                    &nbsp; Edit Downstream
                                </Menu.Item>
                            </div>
                        </Popover>
                        {isCenterNode && centerEntity && (
                            <Menu.Item key="2" onClick={centerEntity}>
                                <StyledImage src={FocusIcon} alt="focus on entity" />
                                &nbsp; Focus on Entity
                            </Menu.Item>
                        )}
                    </Menu>
                }
                trigger={['click']}
            >
                {menuIcon || <MoreOutlined style={{ fontSize: 18 }} />}
            </Dropdown>
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
