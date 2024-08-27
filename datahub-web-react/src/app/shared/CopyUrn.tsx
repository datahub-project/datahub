import { CheckOutlined, CopyOutlined } from '@ant-design/icons';
import { Button, Tooltip } from 'antd';
import React from 'react';

interface CopyUrnProps {
    urn: string;
    isActive: boolean;
    onClick: () => void;
}

export default function CopyUrn({ urn, isActive, onClick }: CopyUrnProps) {
    if (navigator.clipboard) {
        return (
            <Tooltip title="Copy URN. An URN uniquely identifies an entity on DataHub.">
                <Button
                    icon={isActive ? <CheckOutlined /> : <CopyOutlined />}
                    onClick={() => {
                        navigator.clipboard.writeText(urn);
                        onClick();
                    }}
                />
            </Tooltip>
        );
    }

    return null;
}
