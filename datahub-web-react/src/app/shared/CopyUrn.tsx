import { CopyOutlined } from '@ant-design/icons';
import { Tooltip, Button } from 'antd';
import React from 'react';

type Props = {
    urn: string;
};

export const CopyUrn = ({ urn }: Props) => {
    return (
        <Tooltip title="Copy URN. An URN uniquely identifies an entity on DataHub.">
            <Button
                style={{ marginRight: 16 }}
                icon={<CopyOutlined />}
                onClick={() => {
                    navigator.clipboard.writeText(urn);
                }}
            />
        </Tooltip>
    );
};
