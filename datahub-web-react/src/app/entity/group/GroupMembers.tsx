import { Divider, List, Space, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { CorpUser, EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { PreviewType } from '../Entity';

type Props = {
    members?: Array<CorpUser> | null;
};

const MemberList = styled(List)`
    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
        margin-top: 12px;
        margin-bottom: 28px;
        padding: 24px 32px;
        box-shadow: ${(props) => props.theme.styles['box-shadow']};
    }
`;

export default function GroupMembers({ members }: Props) {
    const entityRegistry = useEntityRegistry();
    const list = members || [];

    // todo: group membership should be paginated or limited in some way. currently we are fetching all users.

    return (
        <>
            <Space direction="vertical" style={{ width: '100%' }} size="middle">
                <Typography.Title style={{ marginTop: 28 }} level={3}>
                    Group Membership
                </Typography.Title>
                <MemberList
                    dataSource={list}
                    split={false}
                    renderItem={(item, index) => (
                        <>
                            <List.Item>
                                {entityRegistry.renderPreview(EntityType.CorpUser, PreviewType.PREVIEW, item)}
                            </List.Item>
                            {index < list.length - 1 && <Divider />}
                        </>
                    )}
                    bordered
                />
            </Space>
        </>
    );
}
