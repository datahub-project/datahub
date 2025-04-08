import { Tooltip } from '@components';
import React, { useContext, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { EmbeddedListSearchModal } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchModal';
import { FilterSet } from '@app/entityV2/shared/components/styled/search/types';
import { useRegisterInsight } from '@app/homeV2/content/tabs/discovery/sections/insight/InsightStatusProvider';
import { useGetSearchAssets } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/useGetSearchAssets';
import { InsightCard } from '@app/homeV2/content/tabs/discovery/sections/insight/shared/InsightCard';
import InsightCardSkeleton from '@app/homeV2/content/tabs/discovery/sections/insight/shared/InsightCardSkeleton';
import { EntityLinkList } from '@app/homeV2/reference/sections/EntityLinkList';
import OnboardingContext from '@app/onboarding/OnboardingContext';

import { EntityType, SortCriterion } from '@types';

export const INSIGHT_CARD_MIN_WIDTH = 340;

const Header = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;
`;

const Title = styled.div`
    font-size: 16px;
    font-weight: bold;
    display: flex;
    align-items: center;
    justify-content: start;
    color: ${ANTD_GRAY[9]};
    white-space: nowrap;
    margin-right: 20px;
`;

const Icon = styled.div`
    display: flex;
    margin-right: 8px;
`;

const ShowAll = styled.div`
    color: ${ANTD_GRAY[8]};
    font-size: 12px;
    font-weight: 700;

    :hover {
        cursor: pointer;
        text-decoration: underline;
    }

    white-space: nowrap;
`;

type Props = {
    id: string;
    title: React.ReactNode;
    icon?: React.ReactNode;
    tip?: React.ReactNode;
    types?: [EntityType];
    query?: string;
    filters?: FilterSet;
    empty?: React.ReactNode;
    sort?: SortCriterion;
};

export const SearchListInsightCard = ({ id, title, icon, tip, query, types, filters, sort, empty }: Props) => {
    const [loaded, setLoaded] = useState(false);
    const { localState } = useUserContext();
    const { selectedViewUrn } = localState;
    const { assets, loading } = useGetSearchAssets(types, query, filters, sort, selectedViewUrn);
    const [showModal, setShowModal] = useState(false);
    const { isUserInitializing } = useContext(OnboardingContext);

    useEffect(() => {
        if (!loading && assets && !loaded) {
            setLoaded(true);
        }
    }, [loaded, loading, assets, setLoaded]);

    // Register the insight module with parent component.
    const isPresent = useMemo(() => (loaded ? !!assets?.length : undefined), [assets, loaded]);
    useRegisterInsight(id, isPresent);

    if (loading || isUserInitializing) {
        return <InsightCardSkeleton />;
    }

    if (!assets.length) {
        return null;
    }

    return (
        <>
            <InsightCard id={id} minWidth={INSIGHT_CARD_MIN_WIDTH} maxWidth={500}>
                <Header>
                    <Tooltip title={tip} showArrow={false} placement="top">
                        <Title>
                            {icon && <Icon>{icon}</Icon>}
                            {title}
                        </Title>
                    </Tooltip>
                    <ShowAll onClick={() => setShowModal(true)}>View all</ShowAll>
                </Header>
                <EntityLinkList entities={assets} loading={false} empty={empty || 'No assets found'} />
            </InsightCard>
            {showModal && (
                <EmbeddedListSearchModal
                    title={title}
                    height="80vh"
                    fixedFilters={filters}
                    onClose={() => setShowModal(false)}
                    sort={sort}
                    entityTypes={types}
                />
            )}
        </>
    );
};
