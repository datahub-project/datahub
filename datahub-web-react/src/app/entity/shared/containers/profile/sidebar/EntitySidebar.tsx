// eslint-disable-next-line rulesdir/no-antd-imports -- no alchemy Skeleton exists; keeps this file textually identical to the fork
import { Divider, Skeleton } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { useBaseEntity, useEntityData } from '@app/entity/shared/EntityContext';
import LastIngested from '@app/entity/shared/containers/profile/sidebar/LastIngested';
import { EntitySidebarSection } from '@app/entity/shared/types';

const ContentContainer = styled.div`
    position: relative;

    & > div {
        &:not(:first-child) {
            border-top: 1px solid ${(props) => props.theme.colors.border};
        }
        padding-top: 20px;
        margin-bottom: 20px;
    }
    &::-webkit-scrollbar {
        height: 12px;
        width: 2px;
        background: ${(props) => props.theme.colors.scrollbarTrack};
    }
    &::-webkit-scrollbar-thumb {
        background: ${(props) => props.theme.colors.scrollbarThumb};
        -webkit-border-radius: 1ex;
        -webkit-box-shadow: ${(props) => props.theme.colors.shadowSm};
    }
`;

const LastIngestedSection = styled.div`
    padding: 12px 0 12px 0;
    margin-bottom: 0;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
`;

const LoadingWrapper = styled.div`
    padding-top: 20px;
`;

const SkeletonDivider = styled(Divider)`
    margin: 10px 0 20px 0;
`;

type Props = {
    sidebarSections: EntitySidebarSection[];
    topSection?: EntitySidebarSection;
    loading?: boolean;
};

export const EntitySidebar = <T,>({ sidebarSections, topSection, loading }: Props) => {
    const { entityData } = useEntityData();
    const baseEntity = useBaseEntity<T>();

    if (loading) {
        return (
            <LoadingWrapper>
                <Skeleton active />
                <SkeletonDivider />
                <Skeleton active />
                <SkeletonDivider />
                <Skeleton active />
                <SkeletonDivider />
                <Skeleton active />
            </LoadingWrapper>
        );
    }

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
                    if (section.display?.visible(entityData, baseEntity) !== true) return null;
                    return <section.component key={`${section.component}`} properties={section.properties} />;
                })}
            </ContentContainer>
        </>
    );
};
