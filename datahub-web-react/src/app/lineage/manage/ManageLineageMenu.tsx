import { ArrowDownOutlined, ArrowUpOutlined, MoreOutlined } from '@ant-design/icons';
import { Dropdown, Menu } from 'antd';
import React, { useState } from 'react';
import { Direction } from '../types';
import ManageLineageModal from './ManageLineageModal';

interface Props {
    entityUrn: string;
}

export default function ManageLineageMenu({ entityUrn }: Props) {
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [lineageDirection, setLineageDirection] = useState<Direction>(Direction.Upstream);

    function manageLineage(direction: Direction) {
        setIsModalVisible(true);
        setLineageDirection(direction);
    }

    return (
        <>
            <Dropdown
                overlay={
                    <Menu>
                        <Menu.Item key="0" onClick={() => manageLineage(Direction.Upstream)}>
                            <ArrowUpOutlined />
                            &nbsp; Manage Upstream
                        </Menu.Item>
                        <Menu.Item key="1" onClick={() => manageLineage(Direction.Downstream)}>
                            <ArrowDownOutlined />
                            &nbsp; Manage Downstream
                        </Menu.Item>
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
