import { Divider, Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { useParams } from 'react-router';
import styled from 'styled-components';

import { decodeUrn } from '@app/entityV2/shared/utils';
import { Message } from '@app/sharedV2/Message';

import { useGetExternalRoleQuery } from '@graphql/accessrole.generated';

const PageContainer = styled.div`
    padding: 32px 100px;
`;

const LoadingMessage = styled(Message)`
    margin-top: 10%;
`;

type RolePageParams = {
    urn: string;
};

const TitleLabel = styled(Text).attrs({ type: 'span', size: 'inherit' })`
    line-height: inherit;
    &&& {
        color: ${(props) => props.theme.colors.textTertiary};
        font-size: 12px;
        display: block;
        line-height: 20px;
        font-weight: 700;
    }
`;

const DescriptionLabel = styled(Text).attrs({ type: 'span', size: 'inherit' })`
    line-height: inherit;
    &&& {
        text-align: left;
        font-weight: bold;
        font-size: 14px;
        line-height: 28px;
        color: ${(props) => props.theme.colors.text};
    }
`;

const TitleText = styled(Text).attrs({ type: 'span', size: 'inherit' })`
    line-height: inherit;
    &&& {
        color: ${(props) => props.theme.colors.text};
        font-weight: 700;
        font-size: 20px;
        line-height: 28px;
        display: inline-block;
        margin: 0px 7px;
    }
`;

// Keeps antd Paragraph's 1em bottom margin.
const Paragraph = styled(Text).attrs({ type: 'p' })`
    margin-bottom: 1em;
`;

export default function RoleEntityProfile() {
    const { t } = useTranslation('entity.types');
    const { t: tf } = useTranslation('common.feedback');
    const { urn: encodedUrn } = useParams<RolePageParams>();
    const urn = decodeUrn(encodedUrn);
    const { data, loading } = useGetExternalRoleQuery({ variables: { urn } });

    return (
        <PageContainer>
            {loading && <LoadingMessage type="loading" content={tf('loading')} />}
            <TitleLabel>{t('role.name')}</TitleLabel>
            <TitleText>{data?.role?.properties?.name}</TitleText>
            <Divider />
            {/* Role Description */}
            <DescriptionLabel>{t('tab.about')}</DescriptionLabel>
            <Paragraph style={{ fontSize: '12px', lineHeight: '15px', padding: '5px 0px' }}>
                {data?.role?.properties?.description}
            </Paragraph>
        </PageContainer>
    );
}
