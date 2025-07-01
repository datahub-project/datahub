import React from 'react';
import styled from 'styled-components/macro';

import { useBaseEntity, useEntityData } from '@app/entity/shared/EntityContext';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import LastIngested from '@app/entity/shared/containers/profile/sidebar/LastIngested';
import { EntitySidebarSection } from '@app/entity/shared/types';

const ContentContainer = styled.div`
    position: relative;

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

const LastIngestedSection = styled.div`
    padding: 12px 0 12px 0;
    margin-bottom: 0;
    border-bottom: 1px solid ${ANTD_GRAY[4]};
`;

type Props = {
    sidebarSections: EntitySidebarSection[];
    topSection?: EntitySidebarSection;
};

export const EntitySidebar = <T,>({ sidebarSections, topSection }: Props) => {
    const { entityData } = useEntityData();
    const baseEntity = useBaseEntity<T>();

    return (
        <>
            {topSection && <topSection.component key={`${topSection.component}`} properties={topSection.properties} />}
            {!!entityData?.lastIngested && (
                <LastIngestedSection>
                    <LastIngested lastIngested={entityData.lastIngested} />
                </LastIngestedSection>
            )}
            <ContentContainer>
                {sidebarSections?.map((section) => {
                    if (section.display?.visible(entityData, baseEntity) !== true) {
                        return null;
                    }
                    return <section.component key={`${section.component}`} properties={section.properties} />;
                })}
            </ContentContainer>
        </>
    );
};
