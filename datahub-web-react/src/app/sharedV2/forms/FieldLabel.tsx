import { Icon, Text, Tooltip, typography } from '@components';
import { Question } from '@phosphor-icons/react/dist/csr/Question';
import React from 'react';
import styled, { useTheme } from 'styled-components';

const LabelContainer = styled.span`
    display: inline-flex;
    gap: 4px;
    align-items: center;
`;

const RequiredMark = styled.span`
    color: ${(props) => props.theme.colors.textError};
    font-family: ${typography.fonts.body};
`;

const TooltipIcon = styled(Icon)`
    cursor: pointer;
    color: ${(props) => props.theme.colors.icon};
`;

interface Props {
    label: string;
    required?: boolean;
    tooltip?: React.ReactNode;
    labelHelper?: React.ReactNode;
    className?: string;
}

export function FieldLabel({ label, required, tooltip, labelHelper, className }: Props) {
    const theme = useTheme();
    return (
        <LabelContainer className={className}>
            <Text size="sm" weight="bold" style={{ color: theme.colors.text }}>
                {label}
            </Text>
            {required && <RequiredMark>*</RequiredMark>}
            {tooltip && (
                <Tooltip title={tooltip}>
                    <TooltipIcon icon={Question} size="sm" weight="regular" />
                </Tooltip>
            )}
            {labelHelper}
        </LabelContainer>
    );
}
