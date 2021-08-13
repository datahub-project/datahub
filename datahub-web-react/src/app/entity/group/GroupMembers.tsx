import { Divider, List, Pagination, Row, Space, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { CorpUser, EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { PreviewType } from '../Entity';

type Props = {
    members?: Array<CorpUser> | null;
    page: number;
    pageSize: number;
    totalResults: number;
    onChangePage: (page: number) => void;
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

const MembersView = styled(Space)`
    width: 100%;
    margin-bottom: 32px;
    padding-top: 28px;
`;

export default function GroupMembers({ members, page, onChangePage, pageSize, totalResults }: Props) {
    const entityRegistry = useEntityRegistry();
    const list = members || [];

    // todo: group membership should be paginated or limited in some way. currently we are fetching all users.

    return (
        <MembersView direction="vertical" size="middle">
            <Typography.Title level={3}>Group Membership</Typography.Title>
            <Row justify="center">
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
                <Pagination
                    current={page}
                    pageSize={pageSize}
                    total={totalResults}
                    showLessItems
                    onChange={onChangePage}
                    showSizeChanger={false}
                />
            </Row>
        </MembersView>
    );
}
