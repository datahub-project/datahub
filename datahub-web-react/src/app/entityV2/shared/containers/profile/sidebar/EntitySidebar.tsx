import { colors } from '@components';
import React from 'react';
import styled from 'styled-components/macro';

import { useBaseEntity, useEntityData } from '@app/entity/shared/EntityContext';
import { EntitySidebarSection, TabContextType, TabRenderType } from '@app/entityV2/shared/types';
import { ENTITY_PROFILE_V2_SIDEBAR_ID } from '@app/onboarding/config/EntityProfileOnboardingConfig';

const Container = styled.div`
    padding: 0 12px 0 12px;
`;

const Content = styled.div`
    position: relative;

    & > div {
        padding-top: 12px;

        padding-bottom: 12px;
        &:not(:last-child) {
            border-bottom: 1px solid;
            border-color: ${colors.gray[100]};
            border-radius: 0px;
        }
    }
    &::-webkit-scrollbar {
        height: 12px;
        width: 2px;
        background: #f2f2f2;
    }
    &::-webkit-scrollbar-thumb {
        background: #cccccc;
        -webkit-box-shadow: 0px 1px 2px rgba(0, 0, 0, 0.75);
    }
`;

type Props = {
    sidebarSections: EntitySidebarSection[];
    topSection?: EntitySidebarSection;
    renderType?: TabRenderType;
    contextType: TabContextType;
};

export const EntitySidebarSections = <T,>({ sidebarSections, topSection, renderType, contextType }: Props) => {
    const { entityData } = useEntityData();
    const baseEntity = useBaseEntity<T>();

    return (
        <Container id={ENTITY_PROFILE_V2_SIDEBAR_ID}>
            {topSection && <topSection.component key={`${topSection.component}`} properties={topSection.properties} />}
            <Content>
                {sidebarSections?.map((section) => {
                    if (section.display?.visible(entityData, baseEntity, contextType) !== true) {
                        return null;
                    }
                    return (
                        <section.component
                            key={`${section.component}`}
                            renderType={renderType}
                            contexType={contextType}
                            properties={section.properties}
                        />
                    );
                })}
            </Content>
        </Container>
    );
};
