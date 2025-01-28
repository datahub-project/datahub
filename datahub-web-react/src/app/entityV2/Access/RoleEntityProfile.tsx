import React from 'react';

import { useParams } from 'react-router';
import { Divider, Typography } from 'antd';
import { grey } from '@ant-design/colors';
import styled from 'styled-components';

import { Message } from '../../shared/Message';
import { decodeUrn } from '../shared/utils';
import { useGetExternalRoleQuery } from '../../../graphql/accessrole.generated';

const PageContainer = styled.div`
    padding: 32px 100px;
`;

const LoadingMessage = styled(Message)`
    margin-top: 10%;
`;

type RolePageParams = {
    urn: string;
};

const TitleLabel = styled(Typography.Text)`
    &&& {
        color: ${grey[2]};
        font-size: 12px;
        display: block;
        line-height: 20px;
        font-weight: 700;
    }
`;

const DescriptionLabel = styled(Typography.Text)`
    &&& {
        text-align: left;
        font-weight: bold;
        font-size: 14px;
        line-height: 28px;
        color: rgb(38, 38, 38);
    }
`;

const TitleText = styled(Typography.Text)`
    &&& {
        color: ${grey[10]};
        font-weight: 700;
        font-size: 20px;
        line-height: 28px;
        display: inline-block;
        margin: 0px 7px;
    }
`;

const { Paragraph } = Typography;

export default function RoleEntityProfile() {
    const { urn: encodedUrn } = useParams<RolePageParams>();
    const urn = decodeUrn(encodedUrn);
    const { data, loading } = useGetExternalRoleQuery({ variables: { urn } });

    return (
        <PageContainer>
            {loading && <LoadingMessage type="loading" content="Loading..." />}
            <TitleLabel>Role</TitleLabel>
            <TitleText>{data?.role?.properties?.name}</TitleText>
            <Divider />
            {/* Role Description */}
            <DescriptionLabel>About</DescriptionLabel>
            <Paragraph style={{ fontSize: '12px', lineHeight: '15px', padding: '5px 0px' }}>
                {data?.role?.properties?.description}
            </Paragraph>
        </PageContainer>
    );
}
