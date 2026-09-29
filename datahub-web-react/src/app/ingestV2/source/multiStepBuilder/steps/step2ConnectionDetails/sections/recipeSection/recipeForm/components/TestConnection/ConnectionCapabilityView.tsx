import { Icon, Text, Tooltip, spacing } from '@components';
import { Check } from '@phosphor-icons/react/dist/csr/Check';
import { Question } from '@phosphor-icons/react/dist/csr/Question';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React from 'react';
import styled from 'styled-components/macro';

const Container = styled.div`
    align-items: start;
    display: flex;
    flex-direction: row;
    gap: ${spacing.xsm};
`;

const IconWrapper = styled.div`
    margin-top: 3px;
`;

const CapabilityInfo = styled.div`
    display: flex;
    flex-direction: column;
`;

const StyledQuestion = styled(Question)`
    color: ${({ theme }) => theme.colors.icon};
    margin-left: 4px;
`;

interface Props {
    success: boolean;
    capability: string;
    displayMessage: string | null;
    tooltipMessage: string | null;
    number?: number;
}

export function ConnectionCapabilityView({ success, capability, displayMessage, tooltipMessage, number }: Props) {
    return (
        <Container>
            <IconWrapper>
                {success ? (
                    <Icon icon={Check} size="2xl" color="iconSuccess" />
                ) : (
                    <Icon icon={X} size="2xl" color="iconError" />
                )}
            </IconWrapper>

            <CapabilityInfo>
                <Text>
                    {number && `${number}. `} {capability}
                </Text>
                <Text size="sm">
                    {displayMessage}
                    {tooltipMessage && (
                        <Tooltip title={tooltipMessage}>
                            <StyledQuestion />
                        </Tooltip>
                    )}
                </Text>
            </CapabilityInfo>
        </Container>
    );
}
