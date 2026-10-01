import { Link as LinkIcon } from '@phosphor-icons/react/dist/csr/Link';
import React from 'react';
import { useTranslation } from 'react-i18next';

import {
    ENTITY_HEADER_ACTION_ICON_SIZE,
    ENTITY_HEADER_ACTION_ICON_WEIGHT,
} from '@app/entityV2/shared/EntityDropdown/styledComponents';
import { useAssertionURNCopyLink } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/hooks';
import { ActionItem } from '@app/shared/actions/ActionItem';

import { Assertion } from '@types';

type Props = {
    assertion: Assertion;
    isExpandedView?: boolean;
    onActionTriggered?: () => void;
};

export const CopyLinkAction = ({ assertion, isExpandedView = false, onActionTriggered }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const onCopyLink = useAssertionURNCopyLink(assertion.urn);
    return (
        <ActionItem
            key="copy-link"
            tip={t('action.copyLinkToAssertion')}
            icon={<LinkIcon size={ENTITY_HEADER_ACTION_ICON_SIZE} weight={ENTITY_HEADER_ACTION_ICON_WEIGHT} />}
            onClick={onCopyLink}
            isExpandedView={isExpandedView}
            actionName={t('action.copyLink')}
            onActionTriggered={onActionTriggered}
        />
    );
};
