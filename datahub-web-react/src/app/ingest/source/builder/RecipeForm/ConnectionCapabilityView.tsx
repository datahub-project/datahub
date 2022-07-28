import { CheckOutlined, CloseOutlined, QuestionCircleOutlined } from '@ant-design/icons';
import { Tooltip } from 'antd';
import React from 'react';
import { green, red } from '@ant-design/colors';
import styled from 'styled-components/macro';

const CapabilityWrapper = styled.div<{ success: boolean }>`
    align-items: center;
    background-color: ${(props) => (props.success ? green[1] : red[1])};
    border-radius: 2px;
    display: flex;
    font-size: 14px;
    margin-bottom: 15px;
    min-height: 60px;
    padding: 5px 10px;
`;

const CapabilityName = styled.span`
    font-weight: bold;
    margin: 0 10px;
    flex: 1;
`;

const CapabilityMessage = styled.span`
    font-size: 12px;
    flex: 2;
    padding-left: 4px;
`;

const StyledQuestion = styled(QuestionCircleOutlined)`
    color: rgba(0, 0, 0, 0.45);
    margin-left: 4px;
`;
interface Props {
    success: boolean;
    capability: string;
    displayMessage: string | null;
    tooltipMessage: string | null;
}

function ConnectionCapabilityView({ success, capability, displayMessage, tooltipMessage }: Props) {
    return (
        <CapabilityWrapper success={success}>
            {success ? <CheckOutlined style={{ color: green[6] }} /> : <CloseOutlined style={{ color: red[6] }} />}
            <CapabilityName>{capability}</CapabilityName>
            <CapabilityMessage>
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
