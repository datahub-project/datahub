import React from 'react';
import { List } from 'antd';
import styled from 'styled-components';
import VisiblitySensor from 'react-visibility-sensor';

import { useEntityRegistry } from '../../../../useEntityRegistry';
import { PreviewType } from '../../../Entity';
import { Entity } from '../../../../../types.generated';
import { ANTD_GRAY } from '../../constants';

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
