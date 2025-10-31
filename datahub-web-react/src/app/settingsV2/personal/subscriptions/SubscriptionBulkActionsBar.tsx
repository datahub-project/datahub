import React from 'react';
import styled from 'styled-components/macro';

import { SubscriptionDeleteBulkActionsBarItem } from '@app/settingsV2/personal/subscriptions/SubscriptionDeleteBulkActionsBarItem';
import { SubscriptionEditEventsBulkActionsBarItem } from '@app/settingsV2/personal/subscriptions/SubscriptionEditEventsBulkActionsBarItem';
import { SubscriptionSelectAllBulkActionsBarItem } from '@app/settingsV2/personal/subscriptions/SubscriptionSelectAllBulkActionsBarItem';
import { SubscriptionListFilter } from '@app/settingsV2/personal/subscriptions/types';
import { Icon, Text } from '@src/alchemy-components';

import { AndFilterInput } from '@types';

const ActionsContainer = styled.div<{ $hasPagination?: boolean }>`
    display: flex;
    padding: 8px;
    justify-content: center;
    align-items: center;
    gap: 8px;
    width: fit-content;
    border-radius: 12px;
    box-shadow: 0px 4px 12px 0px rgba(9, 1, 61, 0.12);
    background-color: white;
    position: absolute;
    left: 50%;
    bottom: ${(props) => (props.$hasPagination ? '44px' : '12px')};
    transform: translateX(-50%);
    z-index: 1000;
`;

const SelectedContainer = styled.div`
    border-radius: 8px;
    border: 1px solid #d9d9d9;
    padding: 6px 8px;
    display: flex;
    align-items: center;
    gap: 4px;
    justify-content: center;
`;

interface Props {
    selectedUrns: string[];
    setSelectedUrns: React.Dispatch<React.SetStateAction<string[]>>;
    refetch: (urnsToExclude?: string[]) => void;
    isPersonal: boolean;
    hasPagination?: boolean;
    selectedFilters: SubscriptionListFilter;
    orFilters: AndFilterInput[];
    totalSubscriptionsCount: number;
}

export const SubscriptionBulkActionsBar = ({
    selectedUrns,
    setSelectedUrns,
    refetch,
    isPersonal,
    hasPagination,
    selectedFilters,
    orFilters,
    totalSubscriptionsCount,
}: Props) => {
    if (selectedUrns.length === 0) {
        return null;
    }

    return (
        <ActionsContainer $hasPagination={hasPagination}>
            <SelectedContainer>
                <Text color="gray">{`${selectedUrns.length} Selected`}</Text>
                <Icon
                    icon="X"
                    source="phosphor"
                    size="md"
                    color="gray"
                    onClick={() => setSelectedUrns([])}
                    style={{ cursor: 'pointer' }}
                />
            </SelectedContainer>
            {totalSubscriptionsCount > selectedUrns.length && (
                <SubscriptionSelectAllBulkActionsBarItem
                    selectedFilters={selectedFilters}
                    orFilters={orFilters}
                    setSelectedUrns={setSelectedUrns}
                    totalSubscriptionsCount={totalSubscriptionsCount}
                />
            )}
            <SubscriptionEditEventsBulkActionsBarItem
                selectedUrns={selectedUrns}
                setSelectedUrns={setSelectedUrns}
                refetch={refetch}
                isPersonal={isPersonal}
            />
            <SubscriptionDeleteBulkActionsBarItem
                selectedUrns={selectedUrns}
                setSelectedUrns={setSelectedUrns}
                refetch={refetch}
                isPersonal={isPersonal}
            />
        </ActionsContainer>
    );
};
