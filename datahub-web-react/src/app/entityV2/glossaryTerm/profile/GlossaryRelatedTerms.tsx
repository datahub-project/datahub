import { Loader } from '@components';
import { Menu } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import GlossaryRelatedTermsResult from '@app/entityV2/glossaryTerm/profile/GlossaryRelatedTermsResult';
import { RelatedTermTypes, getRelatedTermTypeLabel } from '@app/entityV2/glossaryTerm/profile/RelatedTermTypes';

import { useGetRelatedTermsQuery } from '@graphql/glossary.generated';

const RELATED_TERM_MENU_KEYS = Object.keys(RelatedTermTypes);

type RelationshipGroup = {
    relationships?: ReadonlyArray<{ entity?: { urn?: string } | null } | null> | null;
} | null;

function relationshipsForGroup(term: object | null | undefined, key: string) {
    if (!term) {
        return [];
    }
    const group = (term as Record<string, RelationshipGroup>)[key];
    const relationships = group?.relationships;
    return Array.isArray(relationships) ? relationships : [];
}

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
    const { urn } = useEntityData();
    const [selectedKey, setSelectedKey] = useState('');

    // The profile query is cache-first and stays mounted across terms, so reading relationships
    // from it shows the previous term. This query follows the urn on screen.
    const { data, loading, error } = useGetRelatedTermsQuery({
        variables: { urn },
        skip: !urn,
        fetchPolicy: 'cache-and-network',
        notifyOnNetworkStatusChange: true,
    });

    useEffect(() => {
        if (RELATED_TERM_MENU_KEYS.length > 0 && selectedKey.length === 0) {
            setSelectedKey(RELATED_TERM_MENU_KEYS[0]);
        }
    }, [selectedKey]);

    const onMenuClick = ({ key }) => {
        setSelectedKey(key);
    };

    const termForUrn = data?.glossaryTerm?.urn === urn ? data.glossaryTerm : null;
    const relationships = relationshipsForGroup(termForUrn, selectedKey);
    // A cached payload for the previous term is not this page. Keep the loader up until this urn
    // arrives. A finished error still renders the empty tab so the add action is available.
    const showLoader = Boolean(urn) && !termForUrn && loading;

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
                    {RELATED_TERM_MENU_KEYS.map((option) => (
                        <Menu.Item data-testid={option} key={option}>
                            {getRelatedTermTypeLabel(RelatedTermTypes[option])}
                        </Menu.Item>
                    ))}
                </Menu>
            </MenuWrapper>
            <Content>
                {showLoader && <Loader />}
                {!showLoader && selectedKey && (termForUrn || error) && (
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
