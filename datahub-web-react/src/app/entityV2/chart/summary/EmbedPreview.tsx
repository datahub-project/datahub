/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import HeaderIcon from '@mui/icons-material/VisibilityOutlined';
import React from 'react';
import styled from 'styled-components';

import { SummaryTabHeaderTitle, SummaryTabHeaderWrapper } from '@app/entityV2/shared/summary/HeaderComponents';

const Wrapper = styled.div`
    height: fit-content;
`;

const StyledIframe = styled.iframe`
    width: 100%;
    height: 70vh;
`;

interface Props {
    embedUrl: string;
}

export default function EmbedPreview({ embedUrl }: Props) {
    return (
        <Wrapper>
            <SummaryTabHeaderWrapper>
                <SummaryTabHeaderTitle icon={<HeaderIcon />} title="Preview" />
            </SummaryTabHeaderWrapper>
            <StyledIframe src={embedUrl} frameBorder={0} />
        </Wrapper>
    );
}
