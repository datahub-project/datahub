import { DeleteOutlined, EditOutlined, LockOutlined } from '@ant-design/icons';
import { Popconfirm, Tag, Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { colors } from '@src/alchemy-components';

import { OAuthAuthorizationServer } from '@types';

const Card = styled.div`
    border: 1px solid ${colors.gray[100]};
    border-radius: 8px;
    padding: 16px;
    background: white;
    transition: box-shadow 0.2s;

    &:hover {
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    }
`;

const CardHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 8px;
`;

const ServerName = styled.div`
    font-size: 14px;
    font-weight: 600;
    color: ${colors.gray[600]};
    display: flex;
    align-items: center;
    gap: 8px;
`;

const ServerDescription = styled.div`
    font-size: 12px;
    color: ${colors.gray[500]};
    margin-bottom: 12px;
    line-height: 1.4;
`;

const CredentialTypes = styled.div`
    display: flex;
    gap: 4px;
    flex-wrap: wrap;
`;

const Actions = styled.div`
    display: flex;
    gap: 8px;
`;

const ActionButton = styled.button`
    background: none;
    border: none;
    padding: 4px;
    cursor: pointer;
    color: ${colors.gray[500]};
    transition: color 0.2s;

    &:hover {
        color: ${colors.violet[500]};
    }

    &.delete:hover {
        color: ${colors.red[500]};
    }
`;

interface OAuthServerCardProps {
    server: OAuthAuthorizationServer;
    onEdit: () => void;
    onDelete: () => void;
}

const OAuthServerCard: React.FC<OAuthServerCardProps> = ({ server, onEdit, onDelete }) => {
    const { properties } = server;
    const displayName = properties?.displayName || 'Unnamed OAuth Server';
    const description = properties?.description;

    return (
        <Card>
            <CardHeader>
                <ServerName>
                    <LockOutlined />
                    {displayName}
                </ServerName>
                <Actions>
                    <Tooltip title="Edit">
                        <ActionButton onClick={onEdit}>
                            <EditOutlined />
                        </ActionButton>
                    </Tooltip>
                    <Popconfirm
                        title="Delete this OAuth server? This action cannot be undone."
                        onConfirm={onDelete}
                        okText="Delete"
                        cancelText="Cancel"
                        okButtonProps={{ danger: true }}
                    >
                        <Tooltip title="Delete">
                            <ActionButton className="delete">
                                <DeleteOutlined />
                            </ActionButton>
                        </Tooltip>
                    </Popconfirm>
                </Actions>
            </CardHeader>
            {description && <ServerDescription>{description}</ServerDescription>}
            <CredentialTypes>
                <Tag icon={<LockOutlined />} color="blue">
                    OAuth
                </Tag>
            </CredentialTypes>
        </Card>
    );
};

export default OAuthServerCard;
