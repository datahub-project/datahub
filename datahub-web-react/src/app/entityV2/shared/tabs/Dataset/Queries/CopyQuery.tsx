/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { CheckOutlined, CopyOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Button } from 'antd';
import React, { useState } from 'react';

export type Props = {
    query: string;
    showCopyText?: boolean;
    style?: any;
};

export default function CopyQuery({ query, showCopyText = false, style }: Props) {
    const [queryCopied, setQueryCopied] = useState(false);

    const copyQuery = () => {
        navigator.clipboard.writeText(query || '');
        setQueryCopied(true);
    };

    return (
        <Tooltip title="Copy the query">
            <Button onClick={copyQuery} style={style}>
                {showCopyText && ((queryCopied && 'Copied') || 'Copy')}
                {(queryCopied && <CheckOutlined />) || <CopyOutlined />}
            </Button>
        </Tooltip>
    );
}
