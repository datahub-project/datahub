import { LinkOutlined } from '@ant-design/icons';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useAssertionURNCopyLink } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/hooks';
import { ActionItem } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/ActionItem';

import { Assertion } from '@types';

const StyledLinkOutlined = styled(LinkOutlined)`
    && {
        font-size: 12px;
        display: flex;
    }
`;

type Props = {
    assertion: Assertion;
    isExpandedView?: boolean;
};

export const CopyLinkAction = ({ assertion, isExpandedView = false }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const onCopyLink = useAssertionURNCopyLink(assertion.urn);
    return (
        <ActionItem
            key="copy-link"
            tip={t('action.copyLinkToAssertion')}
            icon={<StyledLinkOutlined />}
            onClick={onCopyLink}
            isExpandedView={isExpandedView}
            actionName={t('action.copyLink')}
        />
    );
};
