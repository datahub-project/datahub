import React, { useState } from 'react';
import { CheckOutlined, CopyOutlined } from '@ant-design/icons';
import { Button, Tooltip } from 'antd';
import { useTranslation } from 'react-i18next';

export type Props = {
    query: string;
    showCopyText?: boolean;
    style?: any;
};

export default function CopyQuery({ query, showCopyText = false, style }: Props) {
    const { t } = useTranslation();
    const [queryCopied, setQueryCopied] = useState(false);

    const copyQuery = () => {
        navigator.clipboard.writeText(query || '');
        setQueryCopied(true);
    };

    return (
        <Tooltip title={t('copy.copyQuery')}>
            <Button onClick={copyQuery} style={style}>
                {showCopyText && ((queryCopied && t('common.copied')) || t('common.copy'))}
                {(queryCopied && <CheckOutlined />) || <CopyOutlined />}
            </Button>
        </Tooltip>
    );
}
