import React, { useState } from 'react';

import styled from 'styled-components';
import { CheckOutlined, CopyOutlined } from '@ant-design/icons';

import { ActionItem } from './ActionItem';
import { Assertion } from '../../../../../../../../../types.generated';

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
