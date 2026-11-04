import { Button } from '@components';
import { Info } from '@phosphor-icons/react/dist/csr/Info';
import { Button as AntButton, Drawer } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import TagStyleEntity from '@app/sharedV2/TagStyleEntity';
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
    align-items: center;
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
                        <Button variant="text" color="gray" onClick={closeTagProfileDrawer}>
                            {tc('close')}
                        </Button>
                        {/* broken */}
                        {/* Kept on antd: alchemy Button has no `href`, and this must stay a real link. */}
                        <AntButton href={resolveRuntimePath(entityRegistry.getEntityUrl(EntityType.Tag, urn))}>
                            <Info /> {t('tagDetails')}
                        </AntButton>
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
