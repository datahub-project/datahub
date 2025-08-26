import { LinkOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

import { useAssertionURNCopyLink } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/hooks';
import { ActionItem } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/actions/ActionItem';

import { Assertion } from '@types';

const StyledLinkOutlined = styled(LinkOutlined)`
    && {
        font-size: 12px;
        display: flex;
    }
`;

type Props = {
    assertion: Assertion;
};

export const CopyLinkAction = ({ assertion }: Props) => {
    const onCopyLink = useAssertionURNCopyLink(assertion.urn);
    return (
        <ActionItem
            key="copy-link"
            tip="Copy link to this assertion"
            icon={<StyledLinkOutlined />}
            onClick={onCopyLink}
        />
    );
};
