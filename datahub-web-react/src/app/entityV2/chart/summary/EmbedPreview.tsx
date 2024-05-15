import React from 'react';
import styled from 'styled-components';
import HeaderIcon from '@mui/icons-material/VisibilityOutlined';
import { SummaryTabHeaderTitle, SummaryTabHeaderWrapper } from '../../shared/summary/HeaderComponents';

const Wrapper = styled.div`
    height: fit-content;
`;

const StyledIframe = styled.iframe`
    width: 100%;
    height: 70vh;
`;

interface Props {
    externalUrl: string;
}

export default function EmbedPreview({ externalUrl }: Props) {
    return (
        <Wrapper>
            <SummaryTabHeaderWrapper>
                <SummaryTabHeaderTitle icon={<HeaderIcon />} title="Preview" />
            </SummaryTabHeaderWrapper>
            <StyledIframe src={externalUrl} frameBorder={0} />
        </Wrapper>
    );
}
