/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import HeaderIcon from '@mui/icons-material/VisibilityOutlined';
import React, { useEffect } from 'react';
import styled from 'styled-components';

import { SummaryTabHeaderTitle, SummaryTabHeaderWrapper } from '@app/entityV2/shared/summary/HeaderComponents';

const Wrapper = styled.div`
    height: fit-content;
`;

interface Props {
    externalUrl: string;
}

export default function TableauEmbed({ externalUrl }: Props) {
    useEffect(() => {
        const script = document.createElement('script');
        script.type = 'module';
        script.src = 'https://public.tableau.com/javascripts/api/tableau.embedding.3.latest.min.js';
        script.async = true;

        document.head.appendChild(script);
        return () => {
            document.head.removeChild(script);
        };
    }, []);

    // TODO: Calculate height better (desired: height of tab content)
    return (
        <Wrapper>
            <SummaryTabHeaderWrapper>
                <SummaryTabHeaderTitle icon={<HeaderIcon />} title="Preview" />
            </SummaryTabHeaderWrapper>
            {
                // eslint-disable-next-line @typescript-eslint/ban-ts-comment
                // @ts-ignore
                <tableau-viz id="tableauViz" src={externalUrl} height="70vh" width="100%" />
            }
        </Wrapper>
    );
}
