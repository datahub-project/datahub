import { Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { AssertionGroup } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylTypes';

const Container = styled.div`
    display: flex;
    align-items: center;
    padding: 4px 0px;
    &:hover {
        cursor: pointer;
    }
`;

const TextContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: left;
    font-size: 14px;
`;

const Title = styled(Text)`
    && {
        padding-bottom: 0px;
        margin-bottom: 0px;
    }
`;

const Message = styled(Text)`
    && {
        font-size: 12px;
        margin-left: 8px;
    }
`;

type Props = {
    group: AssertionGroup;
};

export const AssertionGroupHeader = ({ group }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const { summary } = group;
    const inactiveCount = summary.totalAssertions - summary.total;
    const parts = [
        t('assertionList.groupHeaderPassing', { count: summary.passing }),
        t('assertionList.groupHeaderFailing', { count: summary.failing }),
        ...(summary.erroring ? [t('assertionList.groupHeaderErroring', { count: summary.erroring })] : []),
        ...(inactiveCount ? [t('assertionList.groupHeaderInactive', { count: inactiveCount })] : []),
    ];
    const summaryMessage = t('assertionList.groupHeaderSummaryListTemplate', { listItems: parts });
    return (
        <Container>
            <TextContainer>
                <Title type="span" weight="bold">
                    {group.name}
                </Title>
                <Message type="span" color="textSecondary">
                    {summaryMessage}
                </Message>
            </TextContainer>
        </Container>
    );
};
