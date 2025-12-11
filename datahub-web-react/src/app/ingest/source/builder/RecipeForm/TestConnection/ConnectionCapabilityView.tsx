/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { green, red } from '@ant-design/colors';
import { CheckOutlined, CloseOutlined, QuestionCircleOutlined } from '@ant-design/icons';
import { Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';

const CapabilityWrapper = styled.div`
    align-items: center;
    display: flex;
    margin: 10px 0;
`;

const CapabilityName = styled.span`
    color: ${ANTD_GRAY[8]};
    font-size: 18px;
    margin-right: 12px;
`;

const CapabilityMessage = styled.span<{ success: boolean }>`
    color: ${(props) => (props.success ? `${green[6]}` : `${red[5]}`)};
    font-size: 12px;
    flex: 1;
    padding-left: 4px;
`;

const StyledQuestion = styled(QuestionCircleOutlined)`
    color: rgba(0, 0, 0, 0.45);
    margin-left: 4px;
`;

export const StyledCheck = styled(CheckOutlined)`
    color: ${green[6]};
    margin-right: 15px;
`;

export const StyledClose = styled(CloseOutlined)`
    color: ${red[5]};
    margin-right: 15px;
`;

const NumberWrapper = styled.span`
    margin-right: 8px;
`;

interface Props {
    success: boolean;
    capability: string;
    displayMessage: string | null;
    tooltipMessage: string | null;
    number?: number;
}

function ConnectionCapabilityView({ success, capability, displayMessage, tooltipMessage, number }: Props) {
    return (
        <CapabilityWrapper>
            <CapabilityName>
                {success ? <StyledCheck /> : <StyledClose />}
                {number ? <NumberWrapper>{number}.</NumberWrapper> : ''}
                {capability}
            </CapabilityName>
            <CapabilityMessage success={success}>
                {displayMessage}
                {tooltipMessage && (
                    <Tooltip overlay={tooltipMessage}>
                        <StyledQuestion />
                    </Tooltip>
                )}
            </CapabilityMessage>
        </CapabilityWrapper>
    );
}

export default ConnectionCapabilityView;
