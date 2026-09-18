import { Avatar, Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';

import GlossaryTermPill from '@app/glossaryV2/GlossaryTermPill';
import { getGlossaryTermColor, useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import EntityIcon from '@app/searchV2/autoCompleteV2/components/icon/EntityIcon';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { toRelativeTimeString } from '@app/shared/time/timeUtils';
import HoverCardEntityRow from '@app/sharedV2/hoverCard/HoverCardEntityRow';
import HoverCardHeader from '@app/sharedV2/hoverCard/HoverCardHeader';
import HoverCardSection from '@app/sharedV2/hoverCard/HoverCardSection';
import HoverCardAttributionDetails from '@app/sharedV2/propagation/HoverCardAttributionDetails';
import { AttributionDetails } from '@app/sharedV2/propagation/types';
import { hasPropagationDetails } from '@app/sharedV2/propagation/utils';
import TagPill from '@app/sharedV2/tags/TagPill';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { CorpUser, Entity, EntityType } from '@src/types.generated';

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
    display: -webkit-box;
    -webkit-box-orient: vertical;
    -webkit-line-clamp: ${DESCRIPTION_MAX_LINES};
    overflow: hidden;
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

type Props = {
    entity: Entity;
    propagationDetails?: AttributionDetails;
};

export default function EntityHoverCard({ entity, propagationDetails }: Props) {
    const { t } = useTranslation('common.labels');
    const entityRegistry = useEntityRegistryV2();
    const generateGlossaryColor = useGenerateGlossaryColorFromPalette();
    const properties = entityRegistry.getGenericEntityProperties(entity.type, entity);

    const description =
        properties?.documentation?.documentations?.[0]?.documentation ||
        properties?.editableProperties?.description ||
        properties?.properties?.description ||
        // Tags carry their description on the deprecated top-level field rather than under
        // `properties`, and that's what the tag pill fragment selects.
        properties?.description ||
        undefined;

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
        hasPropagationDetails(propagationDetails);

    const crumbs = [
        ...(properties?.parentNodes?.nodes ?? []).map((node) => entityRegistry.getDisplayName(node.type, node)),
        ...(properties?.parentDomains?.domains ?? []).map((parentDomain) =>
            entityRegistry.getDisplayName(parentDomain.type, parentDomain),
        ),
        ...(properties?.parentContainers?.containers ?? []).map((container) =>
            entityRegistry.getDisplayName(container.type, container),
        ),
    ].reverse();

    return (
        <Card $hasSections={hasSections}>
            <HoverCardHeader
                title={entityRegistry.getDisplayName(entity.type, entity)}
                icon={<EntityIcon entity={entity} size={32} />}
                typeName={entityRegistry.getEntityName(entity.type)}
                crumbs={crumbs}
            />
            {hasSections && (
                <Sections>
                    {description && (
                        <HoverCardSection title={t('documentation')}>
                            <Description>
                                <Text size="md">{description}</Text>
                            </Description>
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
        </Card>
    );
}
