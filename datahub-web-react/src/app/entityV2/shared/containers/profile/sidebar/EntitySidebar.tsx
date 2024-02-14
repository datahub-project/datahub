import React from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '../../../constants';
import { useBaseEntity, useEntityData } from '../../../EntityContext';
import { EntitySidebarSection, TabContextType, TabRenderType } from '../../../types';
import LastIngested from './LastIngested';

const Container = styled.div`
    padding: 0px 20px 20px 20px;
`;

const Content = styled.div`
    position: relative;

    & > div {
        &:not(:first-child) {
            border-top: 1px solid ${ANTD_GRAY[4]};
        }
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

const LastIngestedSection = styled.div`
    padding: 12px 0 12px 0;
    margin-bottom: 0;
    border-bottom: 1px solid ${ANTD_GRAY[4]};
`;

type Props = {
    sidebarSections: EntitySidebarSection[];
    topSection?: EntitySidebarSection;
    hideLastSynchronized?: boolean;
    renderType?: TabRenderType;
    contextType: TabContextType;
};

export const EntitySidebarSections = <T,>({
    sidebarSections,
    topSection,
    hideLastSynchronized = false,
    renderType,
    contextType,
}: Props) => {
    const { entityData } = useEntityData();
    const baseEntity = useBaseEntity<T>();

    return (
        <Container>
            {topSection && <topSection.component key={`${topSection.component}`} properties={topSection.properties} />}
            {!hideLastSynchronized && entityData?.lastIngested && (
                <LastIngestedSection>
                    <LastIngested lastIngested={entityData.lastIngested} />
                </LastIngestedSection>
            )}
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
