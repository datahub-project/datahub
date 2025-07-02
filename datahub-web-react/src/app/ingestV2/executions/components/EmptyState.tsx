import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { EmptyContainer } from '@app/govern/structuredProperties/styledComponents';
import EmptyFormsImage from '@src/images/empty-forms.svg?react';

export const TextContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
`;

export enum EmptyReasons {
    FILTERS_APPLIED = 'filtersApplied',
    NO_ITEMS = 'noItems',
}

interface Props {
    reason: EmptyReasons;
}

export default function EmptyState({ reason }: Props) {
    const renderContent = () => {
        switch (reason) {
            case EmptyReasons.FILTERS_APPLIED:
                return (
                    <TextContainer>
                        <Text size="lg" color="gray" weight="bold">
                            No results!
                        </Text>
                        <Text size="sm" color="gray" weight="normal">
                            Try another filters...
                        </Text>
                    </TextContainer>
                );
            case EmptyReasons.NO_ITEMS:
                return (
                    <>
                        <EmptyFormsImage />
                        <Text size="md" color="gray" weight="bold">
                            No executions yet!
                        </Text>
                    </>
                );
            default:
                return null;
        }
    };

    return <EmptyContainer>{renderContent()}</EmptyContainer>;
}
