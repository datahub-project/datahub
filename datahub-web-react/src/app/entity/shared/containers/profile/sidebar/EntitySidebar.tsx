import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '../../../constants';
import { useBaseEntity, useEntityData } from '../../../EntityContext';
import { EntitySidebarSection } from '../../../types';

const ContentContainer = styled.div`
    overflow-y: auto;
    & > div {
        &:not(:first-child) {
            border-top: 1px solid ${ANTD_GRAY[4]};
        }
        padding-top: 20px;
        margin-bottom: 20px;
    }
    &::-webkit-scrollbar {
        height: 12px;
        width: 2px;
        background: #f2f2f2;
    }
    &::-webkit-scrollbar-thumb {
        background: #cccccc;
        -webkit-border-radius: 1ex;
        -webkit-box-shadow: 0px 1px 2px rgba(0, 0, 0, 0.75);
    }
`;

type Props = {
    sidebarSections: EntitySidebarSection[];
    showBrowseBar?: boolean;
};

export const EntitySidebar = <T,>({ sidebarSections, showBrowseBar }: Props) => {
    const { entityData } = useEntityData();
    const baseEntity = useBaseEntity<T>();

    return (
        <ContentContainer style={{ height: `calc(100vh - ${showBrowseBar ? '111px' : '66px'})` }}>
            {sidebarSections?.map((section) => {
                if (section.display?.visible(entityData, baseEntity) !== true) {
                    return null;
                }
                return <section.component key={`${section.component}`} properties={section.properties} />;
            })}
        </ContentContainer>
    );
};
