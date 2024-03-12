import React from 'react';
import styled from 'styled-components';
import { useEntityData } from '../../../EntityContext';
import { ANTD_GRAY } from '../../../constants';
import DescriptionSection from '../../../containers/profile/sidebar/AboutSection/DescriptionSection';
import { getPlatformName } from '../../../utils';

const SourceDescriptionWrapper = styled.div`
    border-top: 1px solid ${ANTD_GRAY[4]};
    padding: 16px 0 16px 32px;
`;

const Title = styled.div`
    font-weight: 700;
    padding-bottom: 16px;
`;

export default function SourceDescription() {
    const { entityData } = useEntityData();
    const platformName = getPlatformName(entityData);
    const sourceDescription = entityData?.properties?.description;

    if (!sourceDescription || !entityData?.platform) return null;

    return (
        <SourceDescriptionWrapper>
            <Title>{platformName ? <span>{platformName}</span> : <>Source</>} Documentation:</Title>
            <DescriptionSection description={sourceDescription} limit={200} isExpandable />
        </SourceDescriptionWrapper>
    );
}
