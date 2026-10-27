import { Loader } from '@components';
import { Menu } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import GlossaryRelatedTermsResult from '@app/entityV2/glossaryTerm/profile/GlossaryRelatedTermsResult';
import { RelatedTermTypes, getRelatedTermTypeLabel } from '@app/entityV2/glossaryTerm/profile/RelatedTermTypes';
import {
    RELATED_TERMS_PAGE_SIZE,
    RELATED_TERM_MENU_KEYS,
    RelatedTermKey,
    relatedTermsCountVariables,
    relationshipGroup,
} from '@app/entityV2/glossaryTerm/profile/relatedTermsQuery';
import { Button } from '@src/alchemy-components';

import { useGetRelatedTermsQuery } from '@graphql/glossary.generated';

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

const LoadMoreRow = styled.div`
    padding: 0 16px 16px;
`;

export default function GlossayRelatedTerms() {
    const { urn } = useEntityData();
    const { t } = useTranslation('common.actions');
    const [selectedKey, setSelectedKey] = useState<RelatedTermKey>(RELATED_TERM_MENU_KEYS[0]);
    const [loadedCounts, setLoadedCounts] = useState<{
        urn: string;
        counts: Partial<Record<RelatedTermKey, number>>;
    }>({ urn: '', counts: {} });
    const countsForUrn = loadedCounts.urn === urn ? loadedCounts.counts : {};

    // The profile query is cache-first and stays mounted across terms, so reading relationships
    // from it shows the previous term. This query follows the urn on screen. Each group asks for
    // another page of 10 only after Show more; unselected groups stay at the first page.
    const { data, loading } = useGetRelatedTermsQuery({
        variables: { urn, ...relatedTermsCountVariables(countsForUrn) },
        skip: !urn,
        fetchPolicy: 'cache-and-network',
        notifyOnNetworkStatusChange: true,
    });

    const onMenuClick = ({ key }: { key: string }) => {
        if ((RELATED_TERM_MENU_KEYS as string[]).includes(key)) {
            setSelectedKey(key as RelatedTermKey);
        }
    };

    const termForUrn = data?.glossaryTerm?.urn === urn ? data.glossaryTerm : null;
    const group = relationshipGroup(termForUrn, selectedKey);
    const relationships = group?.relationships ?? [];
    const total = group?.total ?? 0;
    const requestedCount = countsForUrn[selectedKey] ?? RELATED_TERMS_PAGE_SIZE;
    const hasMore = relationships.length < total;
    const loadingMore = loading && relationships.length < requestedCount && relationships.length > 0;
    // A cached payload for the previous term is not this page. Once this urn's request finishes,
    // render the list, including an empty one when the term or the group is missing.
    const showLoader = Boolean(urn) && loading && !termForUrn;

    const onLoadMore = () => {
        setLoadedCounts({
            urn,
            counts: {
                ...countsForUrn,
                [selectedKey]: requestedCount + RELATED_TERMS_PAGE_SIZE,
            },
        });
    };

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
                {!showLoader && urn && (
                    <>
                        <GlossaryRelatedTermsResult
                            key={`${urn}-${selectedKey}`}
                            glossaryRelatedTermType={RelatedTermTypes[selectedKey]}
                            glossaryRelatedTermResult={relationships}
                        />
                        {hasMore && (
                            <LoadMoreRow>
                                <Button
                                    variant="text"
                                    onClick={onLoadMore}
                                    disabled={loadingMore}
                                    data-testid="load-more-related-terms"
                                >
                                    {t('showMoreCapitalized')}
                                </Button>
                            </LoadMoreRow>
                        )}
                    </>
                )}
            </Content>
        </DetailWrapper>
    );
}
