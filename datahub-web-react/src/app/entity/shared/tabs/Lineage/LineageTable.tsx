import { List } from 'antd';
import React from 'react';
import VisiblitySensor from 'react-visibility-sensor';
import styled from 'styled-components';

import { PreviewType } from '@app/entity/Entity';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity } from '@types';

const LineageList = styled(List)`
    padding-left: 40px;
    padding-right: 40px;
    margin-top: -1px;
    .ant-list-items > .ant-list-item {
        padding-right: 0px;
        padding-left: 0px;
    }
    > .ant-list-header {
        padding-right: 0px;
        padding-left: 0px;
        font-size: 14px;
        font-weight: 600;
        margin-left: -20px;
        border-bottom: none;
        padding-bottom: 0px;
        padding-top: 15px;
    }
` as typeof List;

const ItemPlaceholder = styled.div`
    min-height: 100px;
    width: 100%;
    background-color: ${ANTD_GRAY[2]};
`;

const ListItem = styled(List.Item)`
    paddingtop: 20px;
`;
const TableTitle = styled.span`
    font-size: 20px;
`;

type Props = {
    title: string;
    data?: Entity[];
};

export const LineageTable = ({ data, title }: Props) => {
    const entityRegistry = useEntityRegistry();

    return (
        <LineageList
            bordered
            dataSource={data}
            header={<TableTitle>{title}</TableTitle>}
            renderItem={(item) => (
                <ListItem>
                    <VisiblitySensor partialVisibility>
                        {({ isVisible }) =>
                            isVisible && !!item.type ? (
                                entityRegistry.renderPreview(item.type, PreviewType.PREVIEW, item)
                            ) : (
                                <ItemPlaceholder />
                            )
                        }
                    </VisiblitySensor>
                </ListItem>
            )}
        />
    );
};
