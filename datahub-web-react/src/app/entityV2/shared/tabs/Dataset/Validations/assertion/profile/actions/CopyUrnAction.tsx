import { toast } from '@components';
import { Check } from '@phosphor-icons/react/dist/csr/Check';
import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import {
    ENTITY_HEADER_ACTION_ICON_SIZE,
    ENTITY_HEADER_ACTION_ICON_WEIGHT,
} from '@app/entityV2/shared/EntityDropdown/styledComponents';
import { copyTextToClipboard } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/hooks';
import { ActionItem } from '@app/shared/actions/ActionItem';

import { Assertion } from '@types';

type Props = {
    assertion: Assertion;
    isExpandedView?: boolean;
    onActionTriggered?: () => void;
};

export const CopyUrnAction = ({ assertion, isExpandedView = false, onActionTriggered }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const [isUrnCopied, setIsUrnCopied] = useState(false);
    return (
        <ActionItem
            key="copy-urn"
            tip={t('action.copyUrnToAssertion')}
            onClick={async () => {
                try {
                    await copyTextToClipboard(assertion.urn);
                    setIsUrnCopied(true);
                    toast.success(t('action.urnCopied', { defaultValue: 'URN copied to clipboard!' }));
                } catch {
                    toast.error(t('action.urnCopyFailed', { defaultValue: 'Failed to copy URN to clipboard.' }));
                }
            }}
            icon={
                isUrnCopied ? (
                    <Check size={ENTITY_HEADER_ACTION_ICON_SIZE} weight={ENTITY_HEADER_ACTION_ICON_WEIGHT} />
                ) : (
                    <Copy size={ENTITY_HEADER_ACTION_ICON_SIZE} weight={ENTITY_HEADER_ACTION_ICON_WEIGHT} />
                )
            }
            isExpandedView={isExpandedView}
            actionName={t('action.copyUrn')}
            onActionTriggered={onActionTriggered}
        />
    );
};
