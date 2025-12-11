/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { LinkOutlined } from '@ant-design/icons';
import React from 'react';
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
