import { Button, Tooltip } from '@components';
import { Check } from '@phosphor-icons/react/dist/csr/Check';
import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

type Props = {
    query: string;
    showCopyText?: boolean;
    style?: React.CSSProperties;
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
            <Button
                variant="outline"
                color="gray"
                size="sm"
                onClick={copyQuery}
                style={style}
                icon={{ icon: queryCopied ? Check : Copy }}
                isCircle={!showCopyText}
                aria-label={showCopyText ? undefined : tc('copy')}
            >
                {showCopyText && (queryCopied ? tc('common.feedback:copied') : tc('copy'))}
            </Button>
        </Tooltip>
    );
}
