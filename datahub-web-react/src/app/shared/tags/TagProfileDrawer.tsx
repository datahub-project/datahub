/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { InfoCircleOutlined } from '@ant-design/icons';
import { Button, Drawer, Space } from 'antd';
import React from 'react';
import styled from 'styled-components';

import TagStyleEntity from '@app/shared/TagStyleEntity';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

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
                            {/* broken */}
                            <Button href={resolveRuntimePath(entityRegistry.getEntityUrl(EntityType.Tag, urn))}>
                                <InfoCircleOutlined /> Tag Details
                            </Button>
                        </Space>
                    </DetailsLayout>
                }
            >
                <>
                    <TagStyleEntity urn={urn} hideDeleteAction />
                </>
            </Drawer>
        </>
    );
};
