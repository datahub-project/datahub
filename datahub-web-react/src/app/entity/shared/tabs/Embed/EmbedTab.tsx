import { Empty } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../constants';
import { useEntityData } from '../../EntityContext';

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
