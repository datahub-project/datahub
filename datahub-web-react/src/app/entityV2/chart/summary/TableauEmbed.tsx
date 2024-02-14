import React, { useEffect } from 'react';
import styled from 'styled-components';
import HeaderIcon from '@mui/icons-material/VisibilityOutlined';
import { SummaryTabHeaderTitle, SummaryTabHeaderWrapper } from '../../shared/summary/HeaderComponents';

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
