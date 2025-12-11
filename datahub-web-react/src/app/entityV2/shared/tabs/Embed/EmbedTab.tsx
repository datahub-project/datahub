/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Empty } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';

const EmbedContainer = styled.div`
    width: 100%;
    height: 100%;
`;

const StyledIframe = styled.iframe`
    width: 100%;
    height: 100%;
`;

const StyledEmpty = styled(Empty)`
    margin-top: 28px;
    font-size: 16px;
    color: ${ANTD_GRAY[8]};
`;

export const EmbedTab = () => {
    const { entityData } = useEntityData();
    const embedRenderUrl = entityData?.embed?.renderUrl;
    return (
        <EmbedContainer>
            {(embedRenderUrl && <StyledIframe src={embedRenderUrl} title={entityData?.urn} frameBorder={0} />) || (
                <StyledEmpty description="No preview was found." />
            )}
        </EmbedContainer>
    );
};
