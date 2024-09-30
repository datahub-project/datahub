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
    isExpandedView?: boolean;
};

export const CopyLinkAction = ({ assertion, isExpandedView = false }: Props) => {
    const onCopyLink = useAssertionURNCopyLink(assertion.urn);
    return (
        <ActionItem
            key="copy-link"
            tip="Copy link to this assertion"
            icon={<StyledLinkOutlined />}
            onClick={onCopyLink}
            isExpandedView={isExpandedView}
            actionName="Copy link"
        />
    );
};
