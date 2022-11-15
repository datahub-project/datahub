import React, { ReactNode, useState } from 'react';
import styled from 'styled-components';
import { CheckOutlined, CopyOutlined, LinkOutlined, MailOutlined, ShareAltOutlined } from '@ant-design/icons';
import { Button, Dropdown, Menu, Tooltip, Typography } from 'antd';
import EmailShare from 'react-email-share-link';
import MenuItem from 'antd/lib/menu/MenuItem';
import { EntityType } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';

interface ShareButtonProps {
    urn: string;
    entityType: EntityType;
    subType?: string | null;
    name?: string | null;
}

const StyledMenu = styled(Menu)`
    border: 1px solid ${ANTD_GRAY[3]};
    border-radius: 4px;
    min-width: 140px;
`;

export default function ShareButton({ urn, entityType, subType, name }: ShareButtonProps) {
    const [selectedItems, setSelectedItems]: any[] = useState([]);

    const displayName = name || urn;
    const displayType = subType || entityType;

    const menuItems: ReactNode[] = [];

    const addSelectedItem = (index) => {
        setSelectedItems([...selectedItems, index]);
    };

    if (navigator.clipboard) {
        menuItems.push(
            <MenuItem
                key="0"
                onClick={() => {
                    navigator.clipboard.writeText(window.location.href);
                    addSelectedItem(0);
                }}
            >
                <Tooltip title="Copy a shareable link to this entity.">
                    <span style={{ color: ANTD_GRAY[8] }}>
                        {selectedItems.includes(0) ? <CheckOutlined /> : <LinkOutlined style={{ fontSize: 14 }} />}
                        <Typography.Text strong>Copy Link</Typography.Text>
                    </span>
                </Tooltip>
            </MenuItem>,
        );
        menuItems.push(
            <MenuItem
                key="1"
                onClick={() => {
                    navigator.clipboard.writeText(urn);
                    addSelectedItem(1);
                }}
            >
                <Tooltip title="Copy URN. An URN uniquely identifies an entity on DataHub.">
                    <span style={{ color: ANTD_GRAY[8] }}>
                        {selectedItems.includes(1) ? <CheckOutlined /> : <CopyOutlined style={{ fontSize: 14 }} />}
                        <Typography.Text strong>Copy URN</Typography.Text>
                    </span>
                </Tooltip>
            </MenuItem>,
        );
    }

    menuItems.push(
        <MenuItem key="2">
            <EmailShare subject={`${displayName} | ${displayType}`} body={`Check out this ${displayType} on DataHub!`}>
                {(link) => (
                    <a href={link} data-rel="external" onClick={() => addSelectedItem(2)}>
                        <span style={{ color: ANTD_GRAY[8] }}>
                            {selectedItems.includes(2) ? <CheckOutlined /> : <MailOutlined style={{ fontSize: 14 }} />}
                            <Typography.Text strong>Email</Typography.Text>
                        </span>
                    </a>
                )}
            </EmailShare>
        </MenuItem>,
    );

    return (
        <Dropdown
            trigger={['click']}
            overlay={
                <>
                    <StyledMenu selectable={false}>{menuItems}</StyledMenu>
                </>
            }
        >
            <Button type="primary" style={{ paddingLeft: 10, paddingRight: 10 }}>
                <ShareAltOutlined style={{ fontSize: 14 }} />
                Share
            </Button>
        </Dropdown>
    );
}
