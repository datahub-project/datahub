import { CheckOutlined, CopyOutlined } from '@ant-design/icons';
import { message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { copyTextToClipboard } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/hooks';
import { ActionItem } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/ActionItem';

import { Assertion } from '@types';

const StyledCheckOutlined = styled(CheckOutlined)`
    && {
        font-size: 12px;
        display: flex;
    }
`;

const StyledCopyOutlined = styled(CopyOutlined)`
    && {
        font-size: 12px;
        display: flex;
    }
`;

type Props = {
    assertion: Assertion;
    isExpandedView?: boolean;
};

export const CopyUrnAction = ({ assertion, isExpandedView = false }: Props) => {
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
                    message.success(t('action.urnCopied', { defaultValue: 'URN copied to clipboard!' }));
                } catch {
                    message.error(t('action.urnCopyFailed', { defaultValue: 'Failed to copy URN to clipboard.' }));
                }
            }}
            icon={isUrnCopied ? <StyledCheckOutlined /> : <StyledCopyOutlined />}
            isExpandedView={isExpandedView}
            actionName={t('action.copyUrn')}
        />
    );
};
