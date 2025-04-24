import { Empty, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { NewTestButton } from '@app/tests/NewTestButton';

const StyledEmpty = styled(Empty)`
    padding: 80px;
    font-size: 16px;
`;

const StyledParagraph = styled(Typography.Paragraph)`
    && {
        margin-bottom: 20px;
    }
`;

export type Props = {
    readOnly?: boolean;
};

export default function EmptyTests({ readOnly = false }: Props) {
    return (
        <StyledEmpty description="No tests found.">
            <StyledParagraph type="secondary">
                Create a new test to start monitoring your most important data assets.
            </StyledParagraph>
            {!readOnly && <NewTestButton />}
        </StyledEmpty>
    );
}
