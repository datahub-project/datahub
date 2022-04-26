import React from 'react';
import { Drawer, Button, Space } from 'antd';
import styled from 'styled-components';
import { InfoCircleOutlined } from '@ant-design/icons';

import TagStyleEntity from '../TagStyleEntity';

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
    return (
        <>
            <Drawer
                width={500}
                placement="right"
                closable={false}
                onClose={closeTagProfileDrawer}
                visible={tagProfileDrawerVisible}
                footer={
                    <DetailsLayout>
                        <Space>
                            <Button type="text" onClick={closeTagProfileDrawer}>
                                Close
                            </Button>
                        </Space>
                        <Space>
                            <Button href={`/tag/${urn}`}>
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
