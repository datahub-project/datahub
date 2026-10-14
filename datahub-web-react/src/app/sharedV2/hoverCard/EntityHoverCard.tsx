import { Avatar, Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';

import { getRelatedEntitiesUrl as getBusinessAttributeRelatedEntitiesUrl } from '@app/businessAttribute/businessAttributeUtils';
import { getRelatedAssetsUrl } from '@app/entityV2/glossaryTerm/utils';
import { getDisplayedEntityType } from '@app/entityV2/shared/containers/profile/header/utils';
import CompactMarkdownViewer from '@app/entityV2/shared/tabs/Documentation/components/CompactMarkdownViewer';
import GlossaryTermPill from '@app/glossaryV2/GlossaryTermPill';
import { getGlossaryTermColor, useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import EntityIcon from '@app/searchV2/autoCompleteV2/components/icon/EntityIcon';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { toRelativeTimeString } from '@app/shared/time/timeUtils';
import HoverCardEntityRow from '@app/sharedV2/hoverCard/HoverCardEntityRow';
import HoverCardFooter from '@app/sharedV2/hoverCard/HoverCardFooter';
import HoverCardHeader from '@app/sharedV2/hoverCard/HoverCardHeader';
import HoverCardLinks, { HoverCardLink } from '@app/sharedV2/hoverCard/HoverCardLinks';
import HoverCardSection from '@app/sharedV2/hoverCard/HoverCardSection';
import HoverCardStatusBadges from '@app/sharedV2/hoverCard/HoverCardStatusBadges';
import useGroupMemberCount from '@app/sharedV2/hoverCard/useGroupMemberCount';
import HoverCardAttributionDetails from '@app/sharedV2/propagation/HoverCardAttributionDetails';
import { AttributionDetails } from '@app/sharedV2/propagation/types';
import { hasPropagationDetails } from '@app/sharedV2/propagation/utils';
import TagPill from '@app/sharedV2/tags/TagPill';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { CorpUser, Entity, EntityType } from '@src/types.generated';
import { resolveRuntimePath } from '@src/utils/runtimeBasePath';

/** Keeps long documentation from turning the card into a wall of text. */
const DESCRIPTION_MAX_LINES = 5;

/**
 * The card owns its own dimensions so every surface that shows an entity on hover gets the same
 * one — callers render `<EntityHoverCard />` and pass no styling. Full cards keep a floor width so
 * section rows don't look cramped; header-only cards shrink-wrap so Floating UI can center them
 * over small triggers (tag/term pills) instead of floating a mostly-empty 336px shell.
 */
const Card = styled.div<{ $hasSections: boolean }>`
    display: flex;
    flex-direction: column;
    gap: 8px;
    width: max-content;
    ${({ $hasSections }) => $hasSections && 'min-width: 336px;'}
    max-width: min(calc(100vw - 32px), 476px);
`;

const Sections = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    width: 100%;
`;

const Description = styled.div`
    overflow-wrap: anywhere;
`;

const PillRow = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
`;

const AttributionTime = styled.div`
    display: flex;
    align-items: center;
    color: ${(props) => props.theme.colors.textTertiary};
`;

/** The ownership role this person or group holds on the asset the hover was opened from. */
export type HoverCardOwnershipRole = {
    name: string;
    description?: string | null;
};

type Props = {
    entity: Entity;
    propagationDetails?: AttributionDetails;
    ownershipRole?: HoverCardOwnershipRole;
};

export default function EntityHoverCard({ entity, propagationDetails, ownershipRole }: Props) {
    const { t } = useTranslation('common.labels');
    const { t: tTypes } = useTranslation('entity.types');
    const entityRegistry = useEntityRegistryV2();
    const generateGlossaryColor = useGenerateGlossaryColorFromPalette();
    const properties = entityRegistry.getGenericEntityProperties(entity.type, entity);

    // Same preference as the search card: what someone typed in the UI wins over what ingestion
    // brought in, and the `documentation` aspect is only a fallback when both are empty.
    const description =
        properties?.editableProperties?.description ||
        properties?.properties?.description ||
        properties?.documentation?.documentations?.[0]?.documentation ||
        // Tags carry their description on the deprecated top-level field rather than under
        // `properties`, and that's what the tag pill fragment selects.
        properties?.description ||
        undefined;

    // People get their job title where other entities show the type name; it's what the previous
    // user hover card led with, and every owner fragment already fetches it. Same precedence as
    // `UserEntity.renderPreview`.
    const jobTitle =
        entity.type === EntityType.CorpUser
            ? (entity as CorpUser).editableProperties?.title ||
              (entity as CorpUser).properties?.title ||
              (entity as CorpUser).info?.title ||
              undefined
            : undefined;

    const memberCount = useGroupMemberCount(entity);
    const subtitle =
        jobTitle || (memberCount != null ? tTypes('shared.membersCount', { count: memberCount }) : undefined);

    // Same links the old glossary-term and business-attribute hover cards put in the footer.
    const relatedLinks: HoverCardLink[] = [];
    if (entity.type === EntityType.GlossaryTerm) {
        relatedLinks.push({
            href: resolveRuntimePath(getRelatedAssetsUrl(entityRegistry, entity.urn)),
            label: tTypes('glossaryTerm.viewRelatedAssets'),
        });
    } else if (entity.type === EntityType.BusinessAttribute) {
        relatedLinks.push({
            href: resolveRuntimePath(getBusinessAttributeRelatedEntitiesUrl(entityRegistry, entity.urn)),
            label: tTypes('businessAttribute.viewRelatedEntities'),
        });
    }

    const owners = properties?.ownership?.owners ?? [];
    const tags = properties?.globalTags?.tags ?? [];
    const terms = properties?.glossaryTerms?.terms ?? [];
    const domain = properties?.domain?.domain;
    const dataProduct = properties?.dataProduct?.relationships?.[0]?.entity;

    // Who attached this tag/term/owner/domain to the asset being hovered from, and when. Already
    // fetched alongside every association; `HoverCardAttributionDetails` below only surfaces the
    // propagated case, so without this the actor and timestamp go unused.
    const attributionActor = propagationDetails?.attribution?.actor;
    const attributionTime = propagationDetails?.attribution?.time;
    // `actor` is typed as the bare `Entity` interface, so narrow before reaching for the avatar.
    const attributionActorImage =
        attributionActor?.type === EntityType.CorpUser
            ? (attributionActor as CorpUser).editableProperties?.pictureLink
            : undefined;

    // Must track exactly what renders below. Anything counted here that turns out to render
    // nothing leaves an empty `Sections` box, and the card's row gap then pads the header from
    // the bottom of the card — the card stops looking vertically centred. Note the propagation
    // section renders only for propagated attribution, not whenever attribution exists.
    const hasSections =
        !!description ||
        owners.length > 0 ||
        tags.length > 0 ||
        terms.length > 0 ||
        !!domain ||
        !!dataProduct ||
        !!attributionActor ||
        hasPropagationDetails(propagationDetails) ||
        !!ownershipRole;

    // Each parent list comes back nearest-first. Reverse each one on its own so a glossary
    // path, a domain path, and a container path all read root → leaf. Reversing the combined
    // list would do the same to each path and would also swap the order of the three groups.
    const crumbs = [
        ...(properties?.parentContainers?.containers ?? [])
            .map((container) => entityRegistry.getDisplayName(container.type, container))
            .reverse(),
        ...(properties?.parentDomains?.domains ?? [])
            .map((parentDomain) => entityRegistry.getDisplayName(parentDomain.type, parentDomain))
            .reverse(),
        ...(properties?.parentNodes?.nodes ?? [])
            .map((node) => entityRegistry.getDisplayName(node.type, node))
            .reverse(),
    ];

    return (
        // The card renders in a portal, but React still bubbles its clicks to the trigger's ancestors.
        // Triggers often sit inside a row-wide router link, which would swallow clicks on the card's own
        // links (e.g. "View in Snowflake") and navigate to the entity page instead.
        <Card $hasSections={hasSections} onClick={(event) => event.stopPropagation()}>
            <HoverCardHeader
                title={entityRegistry.getDisplayName(entity.type, entity)}
                icon={<EntityIcon entity={entity} size={32} />}
                typeName={getDisplayedEntityType(properties, entityRegistry, entity.type)}
                subtitle={subtitle}
                crumbs={crumbs}
                badge={
                    <HoverCardStatusBadges
                        entity={entity}
                        properties={properties}
                        entityUrl={entityRegistry.getEntityUrl(entity.type, entity.urn)}
                    />
                }
            />
            {hasSections && (
                <Sections>
                    {description && (
                        <HoverCardSection title={t('documentation')}>
                            <Description>
                                <CompactMarkdownViewer
                                    content={description}
                                    lineLimit={DESCRIPTION_MAX_LINES}
                                    scrollableY={false}
                                    hideShowMore
                                />
                            </Description>
                        </HoverCardSection>
                    )}
                    {ownershipRole && (
                        <HoverCardSection title={ownershipRole.name}>
                            {ownershipRole.description && <Text size="md">{ownershipRole.description}</Text>}
                        </HoverCardSection>
                    )}
                    {owners.length > 0 && (
                        <HoverCardSection title={t('owners')}>
                            <PillRow>
                                {owners.map((owner) => (
                                    <Avatar
                                        key={owner.owner.urn}
                                        name={entityRegistry.getDisplayName(owner.owner.type, owner.owner)}
                                        imageUrl={
                                            'editableProperties' in owner.owner
                                                ? owner.owner.editableProperties?.pictureLink
                                                : undefined
                                        }
                                        type={
                                            owner.owner.type === EntityType.CorpGroup
                                                ? AvatarType.group
                                                : AvatarType.user
                                        }
                                        showInPill
                                        size="sm"
                                    />
                                ))}
                            </PillRow>
                        </HoverCardSection>
                    )}
                    {tags.length > 0 && (
                        <HoverCardSection title={t('tags')}>
                            <PillRow>
                                {tags.map((tag) => (
                                    <TagPill
                                        key={tag.tag.urn}
                                        name={entityRegistry.getDisplayName(EntityType.Tag, tag.tag)}
                                        color={tag.tag.properties?.colorHex}
                                        colorHash={tag.tag.urn}
                                    />
                                ))}
                            </PillRow>
                        </HoverCardSection>
                    )}
                    {terms.length > 0 && (
                        <HoverCardSection title={t('terms')}>
                            <PillRow>
                                {terms.map((term) => (
                                    <GlossaryTermPill
                                        key={term.term.urn}
                                        name={entityRegistry.getDisplayName(EntityType.GlossaryTerm, term.term)}
                                        color={getGlossaryTermColor(term.term, generateGlossaryColor)}
                                    />
                                ))}
                            </PillRow>
                        </HoverCardSection>
                    )}
                    {domain && (
                        <HoverCardSection title={t('domain')}>
                            <HoverCardEntityRow entity={domain} />
                        </HoverCardSection>
                    )}
                    {dataProduct && (
                        <HoverCardSection title={t('dataProduct')}>
                            <HoverCardEntityRow entity={dataProduct} />
                        </HoverCardSection>
                    )}
                    {attributionActor && (
                        <HoverCardSection title={t('addedBy')}>
                            {/* Same avatar pill as the Owners section — an actor is a person here
                                too, so it shouldn't get the heavier entity-row treatment. */}
                            <PillRow>
                                <Avatar
                                    name={entityRegistry.getDisplayName(attributionActor.type, attributionActor)}
                                    imageUrl={attributionActorImage}
                                    type={
                                        attributionActor.type === EntityType.CorpGroup
                                            ? AvatarType.group
                                            : AvatarType.user
                                    }
                                    showInPill
                                    size="sm"
                                />
                                {attributionTime && (
                                    <AttributionTime>
                                        <Text size="md">
                                            {capitalizeFirstLetterOnly(toRelativeTimeString(attributionTime))}
                                        </Text>
                                    </AttributionTime>
                                )}
                            </PillRow>
                        </HoverCardSection>
                    )}
                    {propagationDetails && <HoverCardAttributionDetails propagationDetails={propagationDetails} />}
                </Sections>
            )}
            <HoverCardLinks entity={entity} properties={properties} extraLinks={relatedLinks} />
            <HoverCardFooter entity={entity} properties={properties} />
        </Card>
    );
}
