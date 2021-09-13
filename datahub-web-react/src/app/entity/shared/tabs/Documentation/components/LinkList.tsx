import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { message, Button, List, Typography } from 'antd';
import { LinkOutlined, DeleteOutlined } from '@ant-design/icons';

import { EntityType } from '../../../../../../types.generated';
import { GenericEntityUpdate } from '../../../types';
import { useEntityData, useEntityUpdate } from '../../../EntityContext';

import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { ANTD_GRAY } from '../../../constants';
import { formatDateString } from '../../../containers/profile/utils';

const LinkListItem = styled(List.Item)`
    border-radius: 5px;
    > .ant-btn {
        opacity: 0;
    }
    &:hover {
        background-color: ${ANTD_GRAY[2]};
        > .ant-btn {
            opacity: 1;
        }
    }
`;

const ListOffsetIcon = styled.span`
    margin-left: -18px;
    margin-right: 6px;
`;

export const LinkList = () => {
    const { urn, entityData } = useEntityData();
    const updateEntity = useEntityUpdate<GenericEntityUpdate>();
    const entityRegistry = useEntityRegistry();

    const links = entityData?.institutionalMemory?.elements || [];

    const handleDeleteLink = async (index: number) => {
        const newLinks = links.map((link) => {
            return {
                author: link.author.urn,
                url: link.url,
                description: link.description,
                createdAt: link.created.time,
            };
        });
        newLinks.splice(index, 1);

        try {
            await updateEntity({
                variables: { input: { urn, institutionalMemory: { elements: newLinks } } },
            });
            message.success({ content: 'Link Deleted', duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Error deleting link: \n ${e.message || ''}`, duration: 2 });
            }
        }
    };

    return entityData ? (
        <>
            {links.length > 0 && (
                <List
                    size="large"
                    dataSource={links}
                    renderItem={(link, index) => (
                        <LinkListItem
                            extra={
                                <Button onClick={() => handleDeleteLink(index)} type="text" shape="circle" danger>
                                    <DeleteOutlined />
                                </Button>
                            }
                        >
                            <List.Item.Meta
                                title={
                                    <Typography.Title level={5}>
                                        <a href={link.url} target="_blank" rel="noreferrer">
                                            <ListOffsetIcon>
                                                <LinkOutlined />
                                            </ListOffsetIcon>
                                            {link.description}
                                        </a>
                                    </Typography.Title>
                                }
                                description={
                                    <>
                                        Added {formatDateString(link.created.time)} by{' '}
                                        <Link
                                            to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${
                                                link.author.urn
                                            }`}
                                        >
                                            {link.author.username}
                                        </Link>
                                    </>
                                }
                            />
                        </LinkListItem>
                    )}
                />
            )}
        </>
    ) : null;
};
