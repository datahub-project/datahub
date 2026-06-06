import { CheckOutlined, CopyOutlined } from '@ant-design/icons';
import { Button, Tooltip } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

type Props = {
    query: string;
    showCopyText?: boolean;
    style?: any;
};

export default function CopyQuery({ query, showCopyText = false, style }: Props) {
    const { t } = useTranslation('entity.profile.queries');
    const { t: tc } = useTranslation(['common.actions', 'common.feedback']);
    const [queryCopied, setQueryCopied] = useState(false);

    const copyQuery = () => {
        navigator.clipboard.writeText(query || '');
        setQueryCopied(true);
    };

    return (
        <Tooltip title={t('copyQuery.tooltip')}>
            <Button onClick={copyQuery} style={style}>
                {showCopyText && ((queryCopied && tc('common.feedback:copied')) || tc('copy'))}
                {(queryCopied && <CheckOutlined />) || <CopyOutlined />}
            </Button>
        </Tooltip>
    );
}
