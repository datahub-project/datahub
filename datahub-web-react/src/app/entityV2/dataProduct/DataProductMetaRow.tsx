import { Avatar } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { DomainLink } from '@app/sharedV2/tags/DomainLink';
import Term from '@app/sharedV2/tags/term/Term';
import { formatTimestamp } from '@app/sharedV2/time/utils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { Domain, GlossaryTermAssociation, OwnerType } from '@types';

const MetaRow = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 24px;
    align-items: flex-start;
`;

const MetaItem = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    min-width: 80px;
`;

const MetaLabel = styled.div`
    color: ${(props) => props.theme.colors.text};
    font-size: 12px;
    font-weight: 700;
`;

const MetaValues = styled.div`
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 4px;
`;

const MetaEmpty = styled.span`
    color: ${(props) => props.theme.colors.textSecondary};
`;

const DATE_FORMAT = 'll';

type EntityDataWithCreatedOn = {
    properties?: { createdOn?: { time?: number } };
};

export const DataProductMetaRow = () => {
    const { t: tl } = useTranslation('common.labels');
    const { t: ts } = useTranslation('entity.profile.summary');
    const { entityData } = useEntityData();
    const entityRegistry = useEntityRegistryV2();

    const domain = entityData?.domain?.domain as Domain | undefined;
    const owners = entityData?.ownership?.owners?.map((owner) => owner.owner) ?? [];
    const terms = entityData?.glossaryTerms?.terms ?? [];
    const createdTime = (entityData as EntityDataWithCreatedOn | null)?.properties?.createdOn?.time;

    return (
        <MetaRow>
            <MetaItem>
                <MetaLabel>{tl('domain')}</MetaLabel>
                <MetaValues>{domain ? <DomainLink domain={domain} /> : <MetaEmpty>-</MetaEmpty>}</MetaValues>
            </MetaItem>
            <MetaItem>
                <MetaLabel>{tl('owners')}</MetaLabel>
                <MetaValues>
                    {owners.length === 0 && <MetaEmpty>-</MetaEmpty>}
                    {owners.slice(0, 3).map((owner: OwnerType) => {
                        const displayName = entityRegistry.getDisplayName(owner.type, owner);
                        const avatarUrl = owner.editableProperties?.pictureLink;
                        return (
                            <HoverEntityTooltip key={owner.urn} entity={owner} showArrow={false}>
                                <Link to={entityRegistry.getEntityUrl(owner.type, owner.urn)}>
                                    <Avatar name={displayName} imageUrl={avatarUrl} size="sm" showInPill />
                                </Link>
                            </HoverEntityTooltip>
                        );
                    })}
                    {owners.length > 3 && <MetaEmpty>{tl('plusCount', { count: owners.length - 3 })}</MetaEmpty>}
                </MetaValues>
            </MetaItem>
            <MetaItem>
                <MetaLabel>{ts('properties.terms')}</MetaLabel>
                <MetaValues>
                    {terms.length === 0 && <MetaEmpty>-</MetaEmpty>}
                    {terms.slice(0, 3).map((term: GlossaryTermAssociation) => (
                        <Term key={term.term.urn} term={term} readOnly />
                    ))}
                    {terms.length > 3 && <MetaEmpty>{tl('plusCount', { count: terms.length - 3 })}</MetaEmpty>}
                </MetaValues>
            </MetaItem>
            <MetaItem>
                <MetaLabel>{ts('properties.created')}</MetaLabel>
                <MetaValues>
                    {createdTime ? formatTimestamp(createdTime, DATE_FORMAT) : <MetaEmpty>-</MetaEmpty>}
                </MetaValues>
            </MetaItem>
        </MetaRow>
    );
};
