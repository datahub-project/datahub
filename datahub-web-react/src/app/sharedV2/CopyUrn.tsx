import { Tooltip } from '@components';
import { Check } from '@phosphor-icons/react/dist/csr/Check';
import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import { Button } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

interface CopyUrnProps {
    urn: string;
    isActive?: boolean;
    onClick?: () => void;
}

export default function CopyUrn({ urn, isActive, onClick }: CopyUrnProps) {
    const { t } = useTranslation('shared.misc');
    if (navigator.clipboard) {
        return (
            <Tooltip title={t('copyUrn.tooltip')}>
                <Button
                    icon={isActive ? <Check /> : <Copy />}
                    onClick={() => {
                        navigator.clipboard.writeText(urn);
                        onClick?.();
                    }}
                />
            </Tooltip>
        );
    }

    return null;
}
