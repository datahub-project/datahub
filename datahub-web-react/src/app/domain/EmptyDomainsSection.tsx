import { PlusOutlined, ReadOutlined } from '@ant-design/icons';
import { Button, Empty, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { ANTD_GRAY } from '../entity/shared/constants';

const EmptyDomainContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
`;

const StyledEmpty = styled(Empty)`
    width: 35vw;
    @media screen and (max-width: 1300px) {
        width: 50vw;
    }
    @media screen and (max-width: 896px) {
        overflow-y: auto;
        max-height: 75vh;
        &::-webkit-scrollbar {
            width: 5px;
            background: #d6d6d6;
        }
    }
    padding: 60px 40px;
    .ant-empty-image {
        display: none;
    }
`;

const StyledButton = styled(Button)`
    margin: 18px 8px 0 0;
`;

const StyledParagraph = styled(Typography.Paragraph)<{ alignLeft?: string }>`
    text-align: justify;
    text-justify: inter-word;
    margin: 40px 0;
    font-size: 15px;
`;

interface Props {
    setIsCreatingDomain: React.Dispatch<React.SetStateAction<boolean>>;
}

function EmptyDomainsSection(props: Props) {
    const { setIsCreatingDomain } = props;

    return (
        <EmptyDomainContainer>
            <StyledEmpty
                description={
                    <>
                        <ReadOutlined style={{ fontSize: 40, marginBottom: 10, color: ANTD_GRAY[7] }} />
                        <Typography.Title level={4}>Organize your data</Typography.Title>
                        <StyledParagraph type="secondary">
                            <strong style={{ color: ANTD_GRAY[8] }}>Welcome to your Data Domains!</strong> It looks like
                            this space is ready to be transformed into a well-organized data universe. Start by creating
                            your first domain - a high-level category for your data assets.
                        </StyledParagraph>
                        <StyledParagraph type="secondary">
                            <strong style={{ color: ANTD_GRAY[8] }}> Create Nested Domains:</strong> Want to dive
                            deeper? You can also create nested domains to add granularity and structure. Just like
                            nesting Russian dolls, its all about refining your organization.
                        </StyledParagraph>
                        <StyledParagraph type="secondary">
                            <strong style={{ color: ANTD_GRAY[8] }}>Build Data Products</strong>: Once your domains are
                            set, go a step further! Organize your data assets into data products to realize a data mesh
                            architecture. Data products empower you to treat data as a product, making it more
                            accessible and manageable.
                        </StyledParagraph>
                        <StyledParagraph type="secondary">
                            Ready to embark on this data adventure? Click the Create Domain button to begin shaping your
                            data landscape!
                        </StyledParagraph>
                    </>
                }
            >
                <StyledButton onClick={() => setIsCreatingDomain(true)}>
                    <PlusOutlined /> Create Domain
                </StyledButton>
            </StyledEmpty>
        </EmptyDomainContainer>
    );
}

export default EmptyDomainsSection;
