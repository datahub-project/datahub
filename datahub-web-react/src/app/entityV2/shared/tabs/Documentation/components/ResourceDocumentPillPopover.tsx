import { Tooltip } from '@components';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import AvatarPillWithLinkAndHover from '@components/components/Avatar/AvatarPillWithLinkAndHover';

import { removeMarkdown } from '@app/entityV2/shared/components/styled/StripMarkdownText';
import { formatDateString } from '@app/entityV2/shared/containers/profile/utils';
import { ResourcePillMeta } from '@app/entityV2/shared/tabs/Documentation/components/ResourcePillMeta';
import { toRelativeTimeString } from '@app/shared/time/timeUtils';
import { DomainLink } from '@app/sharedV2/tags/DomainLink';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { Document } from '@types';

const Root = styled.div`
    display: flex;
    flex-direction: column;
    gap: 10px;
    max-width: 320px;
`;

const Summary = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
`;

const Title = styled.div`
    color: ${(props) => props.theme.colors.text};
    font-size: 14px;
    font-weight: 700;
    line-height: 20px;
`;

const Description = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 14px;
    line-height: 20px;
    display: -webkit-box;
    -webkit-box-orient: vertical;
    -webkit-line-clamp: 4;
    overflow: hidden;
`;

const Metadata = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const MetadataSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 0;
`;

const Label = styled.div`
    color: ${(props) => props.theme.colors.text};
    font-size: 12px;
    font-weight: 700;
    line-height: 18px;
`;

const Values = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
`;

type Props = {
    document: Document;
    fallbackTitle: string;
};

export default function ResourceDocumentPillPopover({ document, fallbackTitle }: Props) {
    const { t } = useTranslation('entity.profile.summary');
    const { t: tl } = useTranslation('common.labels');
    const entityRegistry = useEntityRegistryV2();
    const title = document.info?.title || fallbackTitle;
    const description = removeMarkdown(document.info?.contents?.text?.trim() || '');
    const owners = document.ownership?.owners ?? [];
    const domain = document.domain?.domain;
    const lastModified = document.info?.lastModified;
    const actor = lastModified?.actor;
    const relativeTime = toRelativeTimeString(lastModified?.time) || t('links.recently');

    return (
        <Root>
            <Summary>
                <Title>{title}</Title>
                {description && <Description>{description}</Description>}
            </Summary>
            {(owners.length > 0 || domain) && (
                <Metadata>
                    {domain && (
                        <MetadataSection>
                            <Label>{tl('domain')}</Label>
                            <DomainLink
                                domain={domain}
                                readOnly
                                enableTooltip={false}
                                iconSize={20}
                                iconFontSize={12}
                            />
                        </MetadataSection>
                    )}
                    {owners.length > 0 && (
                        <MetadataSection>
                            <Label>{tl('owner')}</Label>
                            <Values>
                                {owners.map(({ owner }) => (
                                    <AvatarPillWithLinkAndHover
                                        key={owner.urn}
                                        user={owner}
                                        size="sm"
                                        entityRegistry={entityRegistry}
                                    />
                                ))}
                            </Values>
                        </MetadataSection>
                    )}
                </Metadata>
            )}
            <ResourcePillMeta
                content={
                    <Trans
                        t={t}
                        i18nKey={actor ? 'links.editedBy' : 'links.edited'}
                        values={{ relativeTime }}
                        components={{
                            time: lastModified?.time ? (
                                <Tooltip title={formatDateString(lastModified.time)}>
                                    <span />
                                </Tooltip>
                            ) : (
                                <span />
                            ),
                        }}
                    />
                }
                actor={actor}
            />
        </Root>
    );
}
