import { CheckCircleOutlined, CloseCircleOutlined, FileTextOutlined } from '@ant-design/icons';
import { Popover, Tag } from 'antd';
import type { TFunction } from 'i18next';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import type { Capability } from '@app/ingestV2/shared/connectorRegistry';
import { SupportStatusBadge } from '@app/ingestV2/source/builder/SupportStatusBadge';

const PopoverContent = styled.div`
    max-width: 320px;
`;

const PopoverHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 12px;
`;

const PopoverTitle = styled.div`
    font-weight: 600;
    font-size: 14px;
    color: ${(props) => props.theme.colors.text};
`;

const CapabilitiesSection = styled.div`
    margin-bottom: 12px;
`;

const SectionLabel = styled.div`
    font-size: 11px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.textTertiary};
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 6px;
`;

const CapabilityList = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
`;

const DocsLink = styled.a`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    font-size: 13px;
    color: ${(props) => props.theme.colors.textBrand};
    text-decoration: none;

    &:hover {
        text-decoration: underline;
    }
`;

// Human-readable, translated capability label (keys live under `capabilities.<ENUM>`).
// Falls back to a prettified enum name for capabilities without an explicit translation.
function getCapabilityLabel(capability: string, t: TFunction<'ingestion.sourceBuilder'>): string {
    return t(`capabilities.${capability}`, {
        defaultValue: capability
            .replace(/_/g, ' ')
            .toLowerCase()
            .replace(/\b\w/g, (c) => c.toUpperCase()),
    });
}

type Props = {
    children: React.ReactElement;
    name: string;
    supportStatus?: string;
    capabilities?: Capability[];
    docsUrl?: string;
};

export const ConnectorDetailPopover = ({ children, name, supportStatus, capabilities, docsUrl }: Props) => {
    const { t } = useTranslation('ingestion.sourceBuilder');
    const supportedCapabilities = capabilities?.filter((c) => c.supported) || [];
    const unsupportedCapabilities = capabilities?.filter((c) => !c.supported) || [];

    // Don't show popover if there's no enrichment data
    if (!supportStatus && supportedCapabilities.length === 0 && !docsUrl) {
        return children;
    }

    const content = (
        <PopoverContent data-testid="connector-detail-popover">
            <PopoverHeader>
                <PopoverTitle>{name}</PopoverTitle>
                <SupportStatusBadge status={supportStatus} />
            </PopoverHeader>
            {supportedCapabilities.length > 0 && (
                <CapabilitiesSection>
                    <SectionLabel>{t('connectorDetail.capabilities')}</SectionLabel>
                    <CapabilityList>
                        {supportedCapabilities.map((cap) => (
                            <Tag key={cap.capability} color="green" icon={<CheckCircleOutlined />}>
                                {getCapabilityLabel(cap.capability, t)}
                            </Tag>
                        ))}
                    </CapabilityList>
                </CapabilitiesSection>
            )}
            {unsupportedCapabilities.length > 0 && (
                <CapabilitiesSection>
                    <SectionLabel>{t('connectorDetail.notSupported')}</SectionLabel>
                    <CapabilityList>
                        {unsupportedCapabilities.map((cap) => (
                            <Tag key={cap.capability} icon={<CloseCircleOutlined />}>
                                {getCapabilityLabel(cap.capability, t)}
                            </Tag>
                        ))}
                    </CapabilityList>
                </CapabilitiesSection>
            )}
            {docsUrl && (
                <DocsLink href={docsUrl} target="_blank" rel="noopener noreferrer">
                    <FileTextOutlined />
                    {t('connectorDetail.viewDocumentation')}
                </DocsLink>
            )}
        </PopoverContent>
    );

    return (
        <Popover content={content} trigger="hover" placement="right" mouseEnterDelay={0.3}>
            {children}
        </Popover>
    );
};
