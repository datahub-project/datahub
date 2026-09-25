import { InfoCircleOutlined } from '@ant-design/icons';
import { Button, Drawer, Space } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
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
    const { t } = useTranslation('shared.tags');
    const { t: tc } = useTranslation('common.actions');
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
                                {tc('close')}
                            </Button>
                        </Space>
                        <Space>
                            {/* broken */}
                            <Button href={resolveRuntimePath(entityRegistry.getEntityUrl(EntityType.Tag, urn))}>
                                <InfoCircleOutlined /> {t('tagDetails')}
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
