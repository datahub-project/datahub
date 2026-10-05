import { Heading, Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import NoMarkdownViewer from '@app/entityV2/shared/components/styled/StripMarkdownText';
import { toLocalDateString } from '@app/shared/time/timeUtils';

const Title = styled(Heading)<{ secondary?: boolean }>`
    && {
        margin: 0px;
        padding: 0px;
        color: ${(props) => (props.secondary && props.theme.colors.textTertiary) || props.theme.colors.text};
    }
    max-height: 40px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const Details = styled.div`
    padding: 0px 20px 0px 20px;
`;

const Header = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    white-space: nowrap;
`;

const Description = styled.div`
    margin-bottom: 16px;
    height: 52px;
    overflow: auto;
`;

const MoreButton = styled.span`
    :hover {
        cursor: pointer;
    }
    font-weight: bold;
`;

const Date = styled.div`
    display: flex;
    justify-content: right;
    align-items: center;
`;

const EmptyText = styled.div`
    && {
        color: ${(props) => props.theme.colors.textTertiary};
    }
`;

type Props = {
    title?: string;
    description?: string;
    createdAtMs?: number;
    onClickExpand?: () => void;
};

export default function QueryCardDetails({ title, description, createdAtMs, onClickExpand }: Props) {
    const { t } = useTranslation('entity.profile.queries');
    const { t: tc } = useTranslation('common.actions');
    return (
        <Details>
            <Header>
                <Title secondary={!title} type="h5">
                    {title || t('queryCard.noTitle')}
                </Title>
            </Header>
            <Description>
                {(description && (
                    <NoMarkdownViewer
                        shouldWrap
                        limit={200}
                        readMore={<MoreButton onClick={onClickExpand}>{tc('more')}</MoreButton>}
                    >
                        {description}
                    </NoMarkdownViewer>
                )) || <EmptyText>{t('queryCard.noDescription')}</EmptyText>}
            </Description>
            <Date>
                {(createdAtMs && (
                    <Text type="span" color="textSecondary">
                        {t('queryCard.createdOn', { date: toLocalDateString(createdAtMs) })}
                    </Text>
                )) ||
                    undefined}
            </Date>
        </Details>
    );
}
