import { ApiOutlined, LinkOutlined } from '@ant-design/icons';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import SignatureTab from '@app/entityV2/api/SignatureTab';
import {
    SectionContainer,
    SummaryTabHeaderTitle,
    SummaryTabWrapper,
} from '@app/entityV2/shared/summary/HeaderComponents';
import SummaryAboutSection from '@app/entityV2/shared/summary/SummaryAboutSection';
import { safeUrl } from '@app/shared/urlUtils';

import { GetApiQuery } from '@graphql/api.generated';

const FactsStrip = styled.div`
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px;
`;

const Fact = styled.span`
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 2px 10px;
    border-radius: 8px;
    font-size: 12px;
    font-weight: 600;
    border: 1px solid ${(props) => props.theme.colors.border};
    color: ${(props) => props.theme.colors.textSecondary};
`;

const RefRow = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
`;

const RefLink = styled.a`
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 13px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.hyperlinks};
    &:hover {
        text-decoration: underline;
    }
`;

/**
 * The default landing tab for an Api. Leads with the typed signature (the
 * defining artifact of a callable, reused from SignatureTab), a facts strip
 * (subtype, HTTP method + path for REST endpoints, platform), a reference row
 * (external docs), and About.
 */
export const ApiSummaryTab = () => {
    const { t } = useTranslation('entity.types');
    const baseEntity = useBaseEntity<GetApiQuery>();
    const api = baseEntity?.entity?.__typename === 'Api' ? baseEntity.entity : undefined;

    const subType = api?.subTypes?.typeNames?.[0];
    const platformName =
        api?.dataPlatformInstance?.platform?.properties?.displayName || api?.dataPlatformInstance?.platform?.name;
    const externalUrl = api?.properties?.externalUrl;
    const restProps = api?.restProperties;

    return (
        <SummaryTabWrapper>
            <FactsStrip data-testid="api-summary-facts">
                {subType && <Fact>{subType}</Fact>}
                {restProps?.method && <Fact>{restProps.method}</Fact>}
                {restProps?.path && <Fact>{restProps.path}</Fact>}
                {platformName && <Fact>{platformName}</Fact>}
            </FactsStrip>

            <SectionContainer>
                <SummaryTabHeaderTitle icon={<ApiOutlined />} title={t('api.summary.signature')} />
                <SignatureTab />
            </SectionContainer>

            {externalUrl && (
                <SectionContainer>
                    <SummaryTabHeaderTitle icon={<LinkOutlined />} title={t('api.summary.reference')} />
                    <RefRow>
                        <RefLink href={safeUrl(externalUrl)} target="_blank" rel="noopener noreferrer">
                            <LinkOutlined />
                            {t('api.summary.viewDocs')}
                        </RefLink>
                    </RefRow>
                </SectionContainer>
            )}

            <SummaryAboutSection />
        </SummaryTabWrapper>
    );
};
