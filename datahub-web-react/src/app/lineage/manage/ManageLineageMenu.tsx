import { ArrowDownOutlined, ArrowUpOutlined, MoreOutlined, PartitionOutlined } from '@ant-design/icons';
import { Button, Dropdown, Menu, Popover } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { Direction } from '../types';
import ManageLineageModal from './ManageLineageModal';

const DROPDOWN_Z_INDEX = 1;
const POPOVER_Z_INDEX = 2;

const StyledButton = styled(Button)`
    margin-top: 10px;
    max-width: min-content;
`;

const ContentWrapper = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
`;

function PopoverContent({ centerEntity, direction }: { centerEntity?: () => void; direction: string }) {
    return (
        <ContentWrapper>
            Focus on this entity to make {direction} edits.
            <StyledButton onClick={centerEntity}>
                <PartitionOutlined /> Click to focus
            </StyledButton>
        </ContentWrapper>
    );
}

interface Props {
    entityUrn: string;
    disableUpstream?: boolean;
    disableDownstream?: boolean;
    centerEntity?: () => void;
}

export default function ManageLineageMenu({ entityUrn, disableUpstream, disableDownstream, centerEntity }: Props) {
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [lineageDirection, setLineageDirection] = useState<Direction>(Direction.Upstream);

    function manageLineage(direction: Direction) {
        setIsModalVisible(true);
        setLineageDirection(direction);
    }

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
                        {(disableUpstream || disableDownstream) && centerEntity && (
                            <Menu.Item key="2" onClick={centerEntity}>
                                <PartitionOutlined />
                                &nbsp; Focus on Entity
                            </Menu.Item>
                        )}
                    </Menu>
                }
                trigger={['click']}
            >
                <MoreOutlined style={{ fontSize: 18 }} />
            </Dropdown>
            {isModalVisible && (
                <ManageLineageModal
                    entityUrn={entityUrn}
                    lineageDirection={lineageDirection}
                    closeModal={() => setIsModalVisible(false)}
                />
            )}
        </>
    );
}
