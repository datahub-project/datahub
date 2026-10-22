import { Loader } from '@components';
import { Menu } from 'antd';
import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components/macro';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import {
    glossaryRelatedTermsArePending,
    glossaryRelationshipSignature,
    pickGlossaryRelatedRelationships,
    relationshipEntries,
} from '@app/entityV2/glossaryTerm/profile/GlossaryRelatedTerms.utils';
import GlossaryRelatedTermsResult from '@app/entityV2/glossaryTerm/profile/GlossaryRelatedTermsResult';
import { RelatedTermTypes, getRelatedTermTypeLabel } from '@app/entityV2/glossaryTerm/profile/RelatedTermTypes';

import { useGetRelatedTermsQuery } from '@graphql/glossary.generated';

// Matches the graph client's default page size. The summary module keeps the query default of 10.
const RELATED_TERMS_PAGE_SIZE = 100;

const DetailWrapper = styled.div`
    display: inline-flex;
    flex: 1;
    width: 100%;
`;

const MenuWrapper = styled.div`
    border-right: 2px solid ${(props) => props.theme.colors.border};
    flex-basis: 30%;
    flex-shrink: 1;
`;

const Content = styled.div`
    flex-grow: 1;
    flex-basis: 70%;
    flex-shrink: 0;
    max-width: 100%;
    overflow: hidden;
`;

export default function GlossayRelatedTerms() {
    const { urn, entityData, loading: profileLoading } = useEntityData();
    const refetch = useRefetch();
    const [selectedKey, setSelectedKey] = useState('');
    const menuOptionsArray = Object.keys(RelatedTermTypes);
    const succeededProfileUrn = useRef<string | null>(null);
    const profileRefetchInFlight = useRef<string | null>(null);
    const profileRefetchAttempts = useRef<{ urn: string; count: number } | null>(null);
    const [profileRefetchAttempt, setProfileRefetchAttempt] = useState(0);
    const seenProfileSignature = useRef<{ urn: string; signature: string } | null>(null);
    const awaitingProfileSignature = useRef<string | null>(null);

    const {
        data,
        loading: fetchedLoading,
        error: fetchedError,
        refetch: refetchRelatedTerms,
    } = useGetRelatedTermsQuery({
        variables: { urn, count: RELATED_TERMS_PAGE_SIZE },
        skip: !urn,
        fetchPolicy: 'cache-and-network',
        notifyOnNetworkStatusChange: true,
    });

    useEffect(() => {
        if (menuOptionsArray && menuOptionsArray.length > 0 && selectedKey.length === 0) {
            setSelectedKey(menuOptionsArray[0]);
        }
    }, [menuOptionsArray, selectedKey]);

    // cache-first can keep the previous term after the route changes, and its own fetch often
    // leaves `loading` false. Refetch only while this urn is still behind, and only remember the
    // urn after that refetch resolves so a failure can try again.
    useEffect(() => {
        if (!urn || profileLoading || entityData?.urn === urn || succeededProfileUrn.current === urn) {
            return;
        }
        if (profileRefetchInFlight.current === urn) {
            return;
        }
        const previousAttempts = profileRefetchAttempts.current;
        const attemptsForUrn = previousAttempts?.urn === urn ? previousAttempts.count : 0;
        // One retry. The empty tab no longer waits on this request, and a rejected refetch must
        // not schedule itself forever.
        if (attemptsForUrn >= 2) {
            return;
        }
        const requested = urn;
        profileRefetchAttempts.current = { urn: requested, count: attemptsForUrn + 1 };
        profileRefetchInFlight.current = requested;
        Promise.resolve()
            .then(() => refetch())
            .then(() => {
                succeededProfileUrn.current = requested;
                if (profileRefetchInFlight.current === requested) {
                    profileRefetchInFlight.current = null;
                }
            })
            .catch(() => {
                if (profileRefetchInFlight.current === requested) {
                    profileRefetchInFlight.current = null;
                }
                setProfileRefetchAttempt((attempt) => attempt + 1);
            });
    }, [urn, profileLoading, entityData?.urn, refetch, profileRefetchAttempt]);

    const profileForUrn =
        entityData?.urn === urn ? (entityData as Record<string, { relationships?: ReadonlyArray<unknown> }>) : null;
    const profileReady = !profileLoading && entityData?.urn === urn;
    const profileSignature = profileForUrn ? glossaryRelationshipSignature(profileForUrn, menuOptionsArray) : '';

    // The first snapshot for a urn is already covered by the query above. A later change is an
    // add or remove, which only refetches the profile.
    useEffect(() => {
        if (!urn || !profileReady) {
            return;
        }
        const seen = seenProfileSignature.current;
        seenProfileSignature.current = { urn, signature: profileSignature };
        if (!seen || seen.urn !== urn) {
            awaitingProfileSignature.current = null;
            return;
        }
        if (seen.signature === profileSignature) {
            return;
        }
        awaitingProfileSignature.current = profileSignature;
        refetchRelatedTerms();
    }, [urn, profileReady, profileSignature, refetchRelatedTerms]);

    const onMenuClick = ({ key }) => {
        setSelectedKey(key);
    };

    const fetchedTerm = data?.glossaryTerm?.urn === urn ? data.glossaryTerm : null;
    const fetchedForUrn = Boolean(data) && (data?.glossaryTerm == null || data.glossaryTerm.urn === urn);
    const fetchedSettled = !fetchedLoading && fetchedForUrn;
    const fetchFailed = Boolean(fetchedError) && !fetchedLoading;
    const fetchedSignature = fetchedTerm
        ? glossaryRelationshipSignature(
              fetchedTerm as Record<string, { relationships?: ReadonlyArray<unknown> }>,
              menuOptionsArray,
          )
        : null;
    if (
        awaitingProfileSignature.current !== null &&
        (fetchFailed || (fetchedSettled && fetchedSignature === awaitingProfileSignature.current))
    ) {
        awaitingProfileSignature.current = null;
    }
    const relationships = pickGlossaryRelatedRelationships(
        relationshipEntries(profileForUrn, selectedKey),
        relationshipEntries(
            fetchedTerm as Record<string, { relationships?: ReadonlyArray<unknown> }> | null,
            selectedKey,
        ),
        {
            fetchedSettled,
            holdForProfile: profileReady && awaitingProfileSignature.current !== null && !fetchFailed,
            fetchFailed,
        },
    );
    const pending = glossaryRelatedTermsArePending({
        urn,
        relationshipCount: relationships.length,
        fetchedSettled,
    });

    return (
        <DetailWrapper>
            <MenuWrapper>
                <Menu
                    selectable={false}
                    mode="inline"
                    selectedKeys={[selectedKey]}
                    onClick={(key) => {
                        onMenuClick(key);
                    }}
                >
                    {menuOptionsArray.map((option) => (
                        <Menu.Item data-testid={option} key={option}>
                            {getRelatedTermTypeLabel(RelatedTermTypes[option])}
                        </Menu.Item>
                    ))}
                </Menu>
            </MenuWrapper>
            <Content>
                {pending && <Loader />}
                {!pending && selectedKey && (profileReady || data?.glossaryTerm?.urn === urn) && (
                    <GlossaryRelatedTermsResult
                        key={`${urn}-${selectedKey}`}
                        glossaryRelatedTermType={RelatedTermTypes[selectedKey]}
                        glossaryRelatedTermResult={[...relationships]}
                    />
                )}
            </Content>
        </DetailWrapper>
    );
}
