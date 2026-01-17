import { DeleteOutlined, EditOutlined } from '@ant-design/icons';
import { Popconfirm, Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { colors } from '@src/alchemy-components';

import { AiPluginConfig, McpTransport } from '@types';

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

const ServiceName = styled.div`
    font-size: 14px;
    font-weight: 600;
    color: ${colors.gray[600]};
`;

const ServiceDescription = styled.div`
    font-size: 12px;
    color: ${colors.gray[500]};
    margin-bottom: 12px;
    line-height: 1.4;
`;

const ServiceUrl = styled.div`
    font-size: 12px;
    font-family: monospace;
    color: ${colors.gray[1700]};
    background: ${colors.gray[100]};
    padding: 4px 8px;
    border-radius: 4px;
    word-break: break-all;
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

const TransportBadge = styled.span`
    font-size: 10px;
    padding: 2px 6px;
    border-radius: 4px;
    background: ${colors.gray[100]};
    color: ${colors.gray[600]};
    text-transform: uppercase;
    margin-left: 8px;
`;

interface ServiceCardProps {
    plugin: AiPluginConfig;
    onEdit: () => void;
    onDelete: () => void;
}

const ServiceCard: React.FC<ServiceCardProps> = ({ plugin, onEdit, onDelete }) => {
    const { service } = plugin;
    const displayName = service?.properties?.displayName || 'Unnamed Service';
    const description = service?.properties?.description;
    const url = service?.mcpServerProperties?.url || '';
    const transport = service?.mcpServerProperties?.transport || McpTransport.Http;

    return (
        <Card>
            <CardHeader>
                <div>
                    <ServiceName>
                        {displayName}
                        <TransportBadge>{transport}</TransportBadge>
                    </ServiceName>
                </div>
                <Actions>
                    <Tooltip title="Edit">
                        <ActionButton onClick={onEdit}>
                            <EditOutlined />
                        </ActionButton>
                    </Tooltip>
                    <Popconfirm
                        title="Delete this MCP server? This action cannot be undone."
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
            {description && <ServiceDescription>{description}</ServiceDescription>}
            <ServiceUrl>{url}</ServiceUrl>
        </Card>
    );
};

export default ServiceCard;
