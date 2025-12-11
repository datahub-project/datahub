/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import DescriptionSection from '@app/entity/shared/containers/profile/sidebar/AboutSection/DescriptionSection';
import { getPlatformName } from '@app/entity/shared/utils';

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
