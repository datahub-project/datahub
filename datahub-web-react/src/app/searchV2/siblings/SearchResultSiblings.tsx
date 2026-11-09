import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { v4 as uuidv4 } from 'uuid';

import { SIBLINGS_SHOWN_INLINE, SiblingGroupData, SiblingMemberData } from '@app/searchV2/siblings/useSiblingGroups';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSiblingGroupsLazyQuery } from '@graphql/siblings.generated';
import { DataPlatform, EntityType } from '@types';

/** Members fetched per page while the expanded list is scrolled. */
export const EXPANDED_PAGE_SIZE = 200;

/** Class on the sibling lists; search's arrow-key navigation leaves keys inside them alone. */
export const SIBLINGS_LIST_CLASS_NAME = 'search-result-siblings';

const Container = styled.div`
    margin: -4px 0 12px 28px;
`;

const SiblingRow = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 12px;
    margin-bottom: 6px;
    background-color: ${(props) => props.theme.colors.bgSurface};
    border: 1px solid ${(props) => props.theme.colors.border};
    border-left: 3px solid ${(props) => props.theme.colors.borderBrand};
    border-radius: 8px;
`;

const Name = styled(Link)`
    font-weight: 600;
    color: ${(props) => props.theme.colors.text};
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    min-width: 0;
    :hover {
        color: ${(props) => props.theme.colors.hyperlinks};
    }
`;

const Muted = styled.span`
    color: ${(props) => props.theme.colors.textSecondary};
    white-space: nowrap;
`;

const PrimaryMarker = styled(Muted)`
    ::before {
        content: '· ';
    }
`;

const Badge = styled.span`
    flex-shrink: 0;
    max-width: 260px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-size: 11px;
    padding: 1px 8px;
    border-radius: 10px;
    color: ${(props) => props.theme.colors.textBrand};
    background-color: ${(props) => props.theme.colors.bgSurfaceBrand};
    border: 1px solid ${(props) => props.theme.colors.borderBrand};
`;

const LinkButton = styled.button`
    background: none;
    border: none;
    padding: 2px 0;
    cursor: pointer;
    color: ${(props) => props.theme.colors.hyperlinks};
`;

const Expanded = styled.div`
    border: 1px solid ${(props) => props.theme.colors.border};
    border-left: 3px solid ${(props) => props.theme.colors.borderBrand};
    border-radius: 8px;
    background-color: ${(props) => props.theme.colors.bgSurface};
`;

const ExpandedHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 8px 12px;
    font-weight: 600;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
`;

const ExpandedList = styled.div`
    max-height: 360px;
    overflow-y: auto;
`;

const CompactRow = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 5px 12px;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
`;

const Location = styled(Muted)`
    margin-left: auto;
    font-size: 12px;
`;

const ErrorText = styled.span`
    color: ${(props) => props.theme.colors.textError};
`;

const Logo = styled.img`
    width: 14px;
    height: 14px;
    object-fit: contain;
    flex-shrink: 0;
`;

const LogoSpacer = styled.span`
    width: 14px;
    flex-shrink: 0;
`;

/** Plain platform logo for compact rows (no color sampling, unlike PlatformIcon). */
function CompactLogo({ member }: { member: SiblingMemberData }) {
    const platform = 'platform' in member.entity ? (member.entity.platform as DataPlatform | null) : null;
    const logoUrl = platform?.properties?.logoUrl || platform?.info?.logoUrl;
    if (!logoUrl) {
        return <LogoSpacer />;
    }
    return <Logo src={logoUrl} alt={platform?.properties?.displayName || platform?.name || ''} />;
}

const Footer = styled.div`
    padding: 6px 12px;
    font-size: 12px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

type Props = {
    /** The search result's dataset urn. */
    urn: string;
    /** The search result's display name, shown in "Sibling of …". */
    resultName: string;
    group: SiblingGroupData;
};

type ExpandedState = {
    members: SiblingMemberData[];
    total: number;
    done: boolean;
    error: boolean;
};

const INITIAL_EXPANDED: ExpandedState = { members: [], total: 0, done: false, error: false };

/**
 * The siblings of one search result, listed under it while "Show siblings" is on: the first few as
 * rows, then "+ N more", which expands to the whole group as a compact, scrollable list.
 */
export default function SearchResultSiblings({ urn, resultName, group }: Props) {
    const { t } = useTranslation('search');
    const entityRegistry = useEntityRegistry();
    // React 17 has no useId; a per-instance uuid gives a stable id that is safe in HTML.
    const listId = useMemo(() => `siblings-${uuidv4()}`, []);
    const [expanded, setExpanded] = useState(false);
    const [state, setState] = useState<ExpandedState>(INITIAL_EXPANDED);
    const [loading, setLoading] = useState(false);
    const requestSeq = useRef(0);
    const listRef = useRef<HTMLDivElement>(null);
    const moreRef = useRef<HTMLButtonElement>(null);
    // Always from the network, so a reset never shows a stale cached page.
    const [fetchPage] = useGetSiblingGroupsLazyQuery({ fetchPolicy: 'network-only' });

    // A new group (or a refetched one) starts the expanded list over; responses still in flight are ignored.
    useEffect(() => {
        requestSeq.current += 1;
        setState(INITIAL_EXPANDED);
        setLoading(false);
    }, [urn, group]);

    const loadPage = useCallback(
        async (start: number) => {
            requestSeq.current += 1;
            const seq = requestSeq.current;
            setLoading(true);
            try {
                const result = await fetchPage({
                    variables: { input: { urns: [urn], start, count: EXPANDED_PAGE_SIZE } },
                });
                if (seq !== requestSeq.current) {
                    return; // superseded by a newer request or a reset
                }
                const page = result.data?.siblingGroups?.[0];
                if (result.error || !page) {
                    setState((current) => ({ ...current, error: true }));
                    return;
                }
                setState((current) => {
                    const seen = new Set(current.members.map((member) => member.entity.urn));
                    const added = page.members.filter((member) => !seen.has(member.entity.urn));
                    const members = start === 0 ? page.members : [...current.members, ...added];
                    return {
                        members,
                        total: page.total,
                        done: page.members.length < EXPANDED_PAGE_SIZE || members.length >= page.total,
                        error: false,
                    };
                });
            } catch (e) {
                if (seq === requestSeq.current) {
                    setState((current) => ({ ...current, error: true }));
                }
            } finally {
                if (seq === requestSeq.current) {
                    setLoading(false);
                }
            }
        },
        [fetchPage, urn],
    );

    useEffect(() => {
        if (expanded && state.members.length === 0 && !state.done && !state.error && !loading) {
            loadPage(0);
        }
    }, [expanded, state, loading, loadPage]);

    // Focus follows the toggle: into the list on expand, back to "+ N more" on collapse.
    const focusAfterToggle = useRef<'list' | 'more' | null>(null);
    useEffect(() => {
        if (focusAfterToggle.current === 'list') listRef.current?.focus();
        if (focusAfterToggle.current === 'more') moreRef.current?.focus();
        focusAfterToggle.current = null;
    }, [expanded]);

    const onScroll = (event: React.UIEvent<HTMLDivElement>) => {
        const list = event.currentTarget;
        const nearBottom = list.scrollTop + list.clientHeight >= list.scrollHeight - 40;
        if (nearBottom && !loading && !state.done && !state.error) {
            loadPage(state.members.length);
        }
    };

    const visibleTotal = group.total;

    if (visibleTotal === 0) {
        return null;
    }

    const memberName = (member: SiblingMemberData) => entityRegistry.getDisplayName(member.entity.type, member.entity);
    const memberUrl = (member: SiblingMemberData) => entityRegistry.getEntityUrl(member.entity.type, member.entity.urn);
    const memberPlatform = (member: SiblingMemberData) =>
        member.entity.type === EntityType.Dataset && 'platform' in member.entity
            ? (member.entity.platform as DataPlatform)
            : undefined;
    const memberLocation = (member: SiblingMemberData) =>
        'container' in member.entity ? member.entity.container?.properties?.name : undefined;

    const retryButton = (onRetry: () => void) => (
        <LinkButton type="button" onClick={onRetry}>
            {t('siblings.retry')}
        </LinkButton>
    );

    if (expanded) {
        const expandedMembers = state.members;
        return (
            <Container id={listId} className={SIBLINGS_LIST_CLASS_NAME} data-testid={`siblings-expanded-${urn}`}>
                <Expanded>
                    <ExpandedHeader>
                        <span>{t('siblings.groupTitle', { count: visibleTotal, name: resultName })}</span>
                        <LinkButton
                            type="button"
                            aria-expanded
                            aria-controls={listId}
                            onClick={() => {
                                focusAfterToggle.current = 'more';
                                setExpanded(false);
                            }}
                        >
                            {t('siblings.showLess')}
                        </LinkButton>
                    </ExpandedHeader>
                    <ExpandedList
                        ref={listRef}
                        tabIndex={0}
                        role="list"
                        aria-label={t('siblings.groupTitle', { count: visibleTotal, name: resultName })}
                        onScroll={onScroll}
                    >
                        {expandedMembers.map((member) => (
                            <CompactRow key={member.entity.urn} role="listitem" data-testid="sibling-compact-row">
                                <CompactLogo member={member} />
                                <Name to={memberUrl(member)}>{memberName(member)}</Name>
                                {member.isPrimary && <PrimaryMarker>{t('siblings.primary')}</PrimaryMarker>}
                                <Location>{memberLocation(member)}</Location>
                            </CompactRow>
                        ))}
                    </ExpandedList>
                    <Footer aria-live="polite">
                        {loading && t('siblings.loading')}
                        {!loading && state.error && (
                            <>
                                <ErrorText>{t('siblings.loadError')}</ErrorText>{' '}
                                {retryButton(() => loadPage(state.members.length))}
                            </>
                        )}
                        {!loading && !state.error && group.truncated && t('siblings.truncated', { count: group.total })}
                    </Footer>
                </Expanded>
            </Container>
        );
    }

    const shown = group.members.slice(0, SIBLINGS_SHOWN_INLINE);
    const more = visibleTotal - shown.length;
    return (
        <Container id={listId} className={SIBLINGS_LIST_CLASS_NAME} data-testid={`siblings-${urn}`}>
            <div role="list" aria-label={t('siblings.groupTitle', { count: visibleTotal, name: resultName })}>
                {shown.map((member) => (
                    <SiblingRow key={member.entity.urn} role="listitem" data-testid="sibling-row">
                        <PlatformIcon platform={memberPlatform(member)} size={16} />
                        <Name to={memberUrl(member)}>{memberName(member)}</Name>
                        {member.isPrimary && <PrimaryMarker>{t('siblings.primary')}</PrimaryMarker>}
                        <Badge>{t('siblings.siblingOf', { name: resultName })}</Badge>
                        <Location>{memberLocation(member)}</Location>
                    </SiblingRow>
                ))}
            </div>
            {more > 0 && (
                <LinkButton
                    ref={moreRef}
                    type="button"
                    aria-expanded={false}
                    aria-controls={listId}
                    onClick={() => {
                        focusAfterToggle.current = 'list';
                        setExpanded(true);
                    }}
                >
                    {t('siblings.moreSiblings', { count: more, name: resultName })}
                </LinkButton>
            )}
        </Container>
    );
}
