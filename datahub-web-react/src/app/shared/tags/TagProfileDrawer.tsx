import { InfoCircleOutlined } from '@ant-design/icons';
import { Button, Drawer, Space } from 'antd';
import React from 'react';
import styled from 'styled-components';

import TagStyleEntity from '@app/shared/TagStyleEntity';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

type Props = {
    closeTagProfileDrawer?: () => void;
    tagProfileDrawerVisible?: boolean;
    urn: string;
};

const DetailsLayout = styled.div`
    display: flex;
    justify-content: space-between;
`;

export const TagProfileDrawer = ({ closeTagProfileDrawer, tagProfileDrawerVisible, urn }: Props) => {
    const entityRegistry = useEntityRegistry();
    return (
        <>
            <Drawer
                width={500}
                placement="right"
                closable={false}
                onClose={closeTagProfileDrawer}
                open={tagProfileDrawerVisible}
                footer={
                    <DetailsLayout>
                        <Space>
                            <Button type="text" onClick={closeTagProfileDrawer}>
                                Close
                            </Button>
                        </Space>
                        <Space>
                            <Button href={entityRegistry.getEntityUrl(EntityType.Tag, urn)}>
                                <InfoCircleOutlined /> Tag Details
                            </Button>
                        </Space>
                    </DetailsLayout>
                }
            >
                <>
                    <TagStyleEntity urn={urn} />
                </>
            </Drawer>
        </>
    );
};
