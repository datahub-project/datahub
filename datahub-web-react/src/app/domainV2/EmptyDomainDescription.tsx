import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { ANTD_GRAY } from '../entity/shared/constants';

const StyledParagraph = styled(Typography.Paragraph)`
    text-align: justify;
    text-justify: inter-word;
    margin: 40px 0;
    font-size: 15px;
`;

function EmptyDomainDescription() {
    return (
        <>
            <StyledParagraph type="secondary">
                <strong style={{ color: ANTD_GRAY[8] }}>Welcome to your Data Domains!</strong> It looks like this space
                is ready to be transformed into a well-organized data universe. Start by creating your first domain - a
                high-level category for your data assets.
            </StyledParagraph>
            <StyledParagraph type="secondary">
                <strong style={{ color: ANTD_GRAY[8] }}> Create Nested Domains:</strong> Want to dive deeper? You can
                also create nested domains to add granularity and structure. Just like nesting Russian dolls, its all
                about refining your organization.
            </StyledParagraph>
            <StyledParagraph type="secondary">
                <strong style={{ color: ANTD_GRAY[8] }}>Build Data Products</strong>: Once your domains are set, go a
                step further! Organize your data assets into data products to realize a data mesh architecture. Data
                products empower you to treat data as a product, making it more accessible and manageable.
            </StyledParagraph>
            <StyledParagraph type="secondary">
                Ready to embark on this data adventure? Click the Create Domain button to begin shaping your data
                landscape!
            </StyledParagraph>
        </>
    );
}

export default EmptyDomainDescription;
