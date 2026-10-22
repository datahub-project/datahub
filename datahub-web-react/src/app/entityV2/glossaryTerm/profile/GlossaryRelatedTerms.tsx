import { Loader } from '@components';
import { Menu } from 'antd';
import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components/macro';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import {
    glossaryRelatedTermsArePending,
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
    const requestedUrn = useRef<string | null>(null);

    const { data, loading: fetchedLoading } = useGetRelatedTermsQuery({
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

    // cache-first keeps the previous term in context after the route changes. Refetch once so the
    // profile payload, including the tab count, catches up to the term on screen.
    useEffect(() => {
        if (!urn || profileLoading || entityData?.urn === urn || requestedUrn.current === urn) {
            return;
        }
        requestedUrn.current = urn;
        refetch();
    }, [urn, profileLoading, entityData?.urn, refetch]);

    const onMenuClick = ({ key }) => {
        setSelectedKey(key);
    };

    const profileForUrn =
        entityData?.urn === urn ? (entityData as Record<string, { relationships?: ReadonlyArray<unknown> }>) : null;
    const fetchedTerm = data?.glossaryTerm?.urn === urn ? data.glossaryTerm : null;
    const relationships = pickGlossaryRelatedRelationships(
        relationshipEntries(profileForUrn, selectedKey),
        relationshipEntries(
            fetchedTerm as Record<string, { relationships?: ReadonlyArray<unknown> }> | null,
            selectedKey,
        ),
    );
    const profileReady = !profileLoading && entityData?.urn === urn;
    const pending = glossaryRelatedTermsArePending({
        urn,
        relationshipCount: relationships.length,
        profileReady,
        fetchedSettled: !fetchedLoading,
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
