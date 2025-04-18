import React from 'react';

import styled from 'styled-components';
import { LinkOutlined } from '@ant-design/icons';

import { ActionItem } from './ActionItem';
import { Assertion } from '../../../../../../../../../types.generated';
import { useAssertionURNCopyLink } from '../../builder/hooks';

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
