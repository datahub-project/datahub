/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { CheckOutlined, CopyOutlined } from '@ant-design/icons';
import React, { useState } from 'react';
import styled from 'styled-components';

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
    const [isUrnCopied, setIsUrnCopied] = useState(false);
    return (
        <ActionItem
            key="copy-urn"
            tip="Copy urn for this assertion"
            onClick={() => {
                navigator.clipboard.writeText(assertion.urn);
                setIsUrnCopied(true);
            }}
            icon={isUrnCopied ? <StyledCheckOutlined /> : <StyledCopyOutlined />}
            isExpandedView={isExpandedView}
            actionName="Copy urn"
        />
    );
};
