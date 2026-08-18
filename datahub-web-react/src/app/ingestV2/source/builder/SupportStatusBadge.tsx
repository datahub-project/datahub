import { ExperimentOutlined, QuestionCircleOutlined, SafetyCertificateOutlined, TeamOutlined } from '@ant-design/icons';
import { Tooltip } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled, { type DefaultTheme } from 'styled-components';

type StatusVariant = 'success' | 'warning' | 'info' | 'error' | 'neutral';

// Map a semantic status variant to the theme's surface/text color tokens.
function variantColors(colors: DefaultTheme['colors'], variant: StatusVariant): { bg: string; text: string } {
    switch (variant) {
        case 'success':
            return { bg: colors.bgSurfaceSuccess, text: colors.textSuccess };
        case 'warning':
            return { bg: colors.bgSurfaceWarning, text: colors.textWarning };
        case 'info':
            return { bg: colors.bgSurfaceInfo, text: colors.textInformation };
        case 'error':
            return { bg: colors.bgSurfaceError, text: colors.textError };
        default:
            return { bg: colors.bgSurface, text: colors.textTertiary };
    }
}

const BadgeContainer = styled.div<{ $variant: StatusVariant }>`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 2px 8px;
    border-radius: 10px;
    font-size: 11px;
    font-weight: 600;
    line-height: 16px;
    background-color: ${(props) => variantColors(props.theme.colors, props.$variant).bg};
    color: ${(props) => variantColors(props.theme.colors, props.$variant).text};
    white-space: nowrap;
`;

type SupportStatusConfig = {
    labelKey: string;
    icon: React.ReactNode;
    variant: StatusVariant;
    tooltipKey: string;
};

const STATUS_CONFIG: Record<string, SupportStatusConfig> = {
    CERTIFIED: {
        labelKey: 'supportStatus.certified.label',
        icon: <SafetyCertificateOutlined />,
        variant: 'success',
        tooltipKey: 'supportStatus.certified.tooltip',
    },
    INCUBATING: {
        labelKey: 'supportStatus.incubating.label',
        icon: <ExperimentOutlined />,
        variant: 'warning',
        tooltipKey: 'supportStatus.incubating.tooltip',
    },
    COMMUNITY: {
        labelKey: 'supportStatus.community.label',
        icon: <TeamOutlined />,
        variant: 'info',
        tooltipKey: 'supportStatus.community.tooltip',
    },
    TESTING: {
        labelKey: 'supportStatus.testing.label',
        icon: <ExperimentOutlined />,
        variant: 'error',
        tooltipKey: 'supportStatus.testing.tooltip',
    },
    UNKNOWN: {
        labelKey: 'supportStatus.unknown.label',
        icon: <QuestionCircleOutlined />,
        variant: 'neutral',
        tooltipKey: 'supportStatus.unknown.tooltip',
    },
};

type Props = {
    status?: string | null;
    showLabel?: boolean;
};

export const SupportStatusBadge = ({ status, showLabel = true }: Props) => {
    const { t } = useTranslation('ingestion.sourceBuilder');
    if (!status || status === 'UNKNOWN') {
        return null;
    }

    const config = STATUS_CONFIG[status] || STATUS_CONFIG.UNKNOWN;

    return (
        <Tooltip title={t(config.tooltipKey)}>
            <BadgeContainer $variant={config.variant}>
                {config.icon}
                {showLabel && t(config.labelKey)}
            </BadgeContainer>
        </Tooltip>
    );
};

// `labelKey` values are resolved via t() by consumers (e.g. the SelectTemplateStep filter chips).
export const FILTER_OPTIONS = [
    { key: 'ALL', labelKey: 'selectTemplate.filter.all' },
    { key: 'CERTIFIED', labelKey: 'supportStatus.certified.label' },
    { key: 'INCUBATING', labelKey: 'supportStatus.incubating.label' },
    { key: 'COMMUNITY', labelKey: 'supportStatus.community.label' },
] as const;

export type SupportStatusFilter = (typeof FILTER_OPTIONS)[number]['key'];
