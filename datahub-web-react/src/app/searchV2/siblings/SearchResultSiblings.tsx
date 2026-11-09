import React, { useCallback, useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { SIBLINGS_SHOWN_INLINE, SiblingGroupData, SiblingMemberData } from '@app/searchV2/siblings/useSiblingGroups';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSiblingGroupsLazyQuery } from '@graphql/siblings.generated';
import { DataPlatform, EntityType } from '@types';

/** Members fetched per page while the expanded list is scrolled. */
export const EXPANDED_PAGE_SIZE = 200;

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

/**
 * The siblings of one search result, listed under it while "Show siblings" is on: the first few as
 * rows, then "+ N more", which expands to the whole group as a compact, scrollable list.
 */
export default function SearchResultSiblings({ urn, resultName, group }: Props) {
    const { t } = useTranslation('search');
    const entityRegistry = useEntityRegistry();
    const [expanded, setExpanded] = useState(false);
    const [allMembers, setAllMembers] = useState<SiblingMemberData[]>([]);
    const [fetchPage, { loading }] = useGetSiblingGroupsLazyQuery({ fetchPolicy: 'network-only' });

    const loadPage = useCallback(
        async (start: number) => {
            const result = await fetchPage({
                variables: { input: { urns: [urn], start, count: EXPANDED_PAGE_SIZE } },
            });
            const page = result.data?.siblingGroups?.[0]?.members || [];
            setAllMembers((loaded) => (start === 0 ? page : [...loaded, ...page]));
        },
        [fetchPage, urn],
    );

    useEffect(() => {
        if (expanded && allMembers.length === 0) {
            loadPage(0);
        }
    }, [expanded, allMembers.length, loadPage]);

    const onScroll = (event: React.UIEvent<HTMLDivElement>) => {
        const list = event.currentTarget;
        const nearBottom = list.scrollTop + list.clientHeight >= list.scrollHeight - 40;
        if (nearBottom && !loading && allMembers.length < group.total) {
            loadPage(allMembers.length);
        }
    };

    if (group.total === 0) {
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

    if (expanded) {
        return (
            <Container data-testid={`siblings-expanded-${urn}`}>
                <Expanded>
                    <ExpandedHeader>
                        <span>{t('siblings.groupTitle', { count: group.total, name: resultName })}</span>
                        <LinkButton type="button" onClick={() => setExpanded(false)}>
                            {t('siblings.showLess')}
                        </LinkButton>
                    </ExpandedHeader>
                    <ExpandedList onScroll={onScroll}>
                        {allMembers.map((member) => (
                            <CompactRow key={member.entity.urn} data-testid="sibling-compact-row">
                                <PlatformIcon platform={memberPlatform(member)} size={14} />
                                <Name to={memberUrl(member)}>{memberName(member)}</Name>
                                {member.isPrimary && <PrimaryMarker>{t('siblings.primary')}</PrimaryMarker>}
                                <Location>{memberLocation(member)}</Location>
                            </CompactRow>
                        ))}
                    </ExpandedList>
                    {(loading || group.truncated) && (
                        <Footer>
                            {loading ? t('siblings.loading') : t('siblings.truncated', { count: group.total })}
                        </Footer>
                    )}
                </Expanded>
            </Container>
        );
    }

    const shown = group.members.slice(0, SIBLINGS_SHOWN_INLINE);
    const more = group.total - shown.length;
    return (
        <Container data-testid={`siblings-${urn}`}>
            {shown.map((member) => (
                <SiblingRow key={member.entity.urn} data-testid="sibling-row">
                    <PlatformIcon platform={memberPlatform(member)} size={16} />
                    <Name to={memberUrl(member)}>{memberName(member)}</Name>
                    {member.isPrimary && <PrimaryMarker>{t('siblings.primary')}</PrimaryMarker>}
                    <Badge>{t('siblings.siblingOf', { name: resultName })}</Badge>
                    <Location>{memberLocation(member)}</Location>
                </SiblingRow>
            ))}
            {more > 0 && (
                <LinkButton type="button" onClick={() => setExpanded(true)}>
                    {t('siblings.moreSiblings', { count: more, name: resultName })}
                </LinkButton>
            )}
        </Container>
    );
}
