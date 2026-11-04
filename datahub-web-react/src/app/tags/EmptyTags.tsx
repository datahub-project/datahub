import { EmptyState } from '@components';
import { Tag } from '@phosphor-icons/react/dist/csr/Tag';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

type Props = {
    isEmptySearch: boolean;
};

const EmptyContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 40px;
`;

const EmptyTags = ({ isEmptySearch }: Props) => {
    const { t } = useTranslation('misc');
    return (
        <EmptyContainer data-testid="tags-not-found">
            <EmptyState
                icon={Tag}
                title={isEmptySearch ? t('tags.emptySearch') : t('tags.empty')}
                description={isEmptySearch ? undefined : t('tags.emptyDescription')}
            />
        </EmptyContainer>
    );
};

export default EmptyTags;
