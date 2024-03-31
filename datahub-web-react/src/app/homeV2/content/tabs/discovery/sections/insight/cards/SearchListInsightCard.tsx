import React, { useState } from 'react';
import styled from 'styled-components';
import { Tooltip } from 'antd';
import { InsightCard } from '../shared/InsightCard';
import { EntityLinkList } from '../../../../../../reference/sections/EntityLinkList';
import { EmbeddedListSearchModal } from '../../../../../../../entityV2/shared/components/styled/search/EmbeddedListSearchModal';
import { ANTD_GRAY } from '../../../../../../../entity/shared/constants';
import { EntityType, SortCriterion } from '../../../../../../../../types.generated';
import { FilterSet } from '../../../../../../../entityV2/shared/components/styled/search/types';
import { useGetSearchAssets } from './useGetSearchAssets';
import { useRegisterInsight } from '../InsightStatusProvider';
import { InsightLoadingCard } from './InsightLoadingCard';

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
    const { assets, loading } = useGetSearchAssets(types, query, filters, sort);
    const [showModal, setShowModal] = useState(false);

    // Register the insight module with parent component.
    useRegisterInsight(title, !!assets?.length);

    if (!assets) {
        return null;
    }

    return (
        <>
            {(loading && <InsightLoadingCard />) || null}
            {(!loading && assets.length && (
                <InsightCard id={id} minWidth={340} maxWidth={500}>
                    <Header>
                        <Tooltip title={tip} showArrow={false} placement="top">
                            <Title>
                                {icon && <Icon>{icon}</Icon>}
                                {title}
                            </Title>
                        </Tooltip>
                        <ShowAll onClick={() => setShowModal(true)}>view all</ShowAll>
                    </Header>
                    <EntityLinkList entities={assets} loading={false} empty={empty || 'No assets found'} />
                </InsightCard>
            )) ||
                null}
            {showModal && (
                <EmbeddedListSearchModal
                    title={title}
                    height="80vh"
                    fixedFilters={filters}
                    onClose={() => setShowModal(false)}
                    sort={sort}
                />
            )}
        </>
    );
};
