import React from 'react';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { Divider, Image, Modal, Steps, Typography } from 'antd';
import pipinstall from '../../images/pipinstall.png';
import recipeExample from '../../images/recipe-example.png';
import ingestExample from '../../images/ingest-example.png';

const StyledModal = styled(Modal)`
    top: 20px;
`;

const StepImage = styled(Image)`
    width: auto;
    object-fit: contain;
    margin-right: 10px;
    background-color: transparent;
    border-radius: 8px;
`;

const GettingStartedParagraph = styled(Typography.Paragraph)`
    font-size: 14px;
    && {
        margin-bottom: 28px;
    }
`;

const SectionTitle = styled(Typography.Title)`
    && {
        margin-bottom: 12px;
    }
`;

type Props = {
    visible: boolean;
    onClose: () => void;
};

export const GettingStartedModal = ({ visible, onClose }: Props) => {
    return (
        <StyledModal onCancel={onClose} width={800} visible={visible} footer={null}>
            <Typography.Title level={3}>Welcome to DataHub</Typography.Title>
            <Divider />
            <Typography.Title level={4}>Getting Started</Typography.Title>
            <GettingStartedParagraph>
                It looks like you&apos;re new to DataHub - Welcome! To start ingesting metadata, follow these steps or
                check out the full{' '}
                <a href="https://datahubproject.io/docs/metadata-ingestion" target="_blank" rel="noreferrer">
                    Metadata Ingestion Quickstart Guide.
                </a>
            </GettingStartedParagraph>
            <SectionTitle level={5}>UI Ingestion</SectionTitle>
            <GettingStartedParagraph>
                Start integrating your data sources immediately by navigating to the{' '}
                <Link to="/ingestion">Ingestion</Link> tab.
            </GettingStartedParagraph>
            <SectionTitle level={5}>CLI Ingestion</SectionTitle>
            <Steps current={-1} direction="vertical">
                <Steps.Step
                    title="Install the DataHub CLI"
                    description={
                        <>
                            <Typography.Paragraph>
                                From your command line, install the acryl-datahub package from PyPI.
                            </Typography.Paragraph>
                            <StepImage preview={false} height={52} src={pipinstall} />
                        </>
                    }
                />
                <Steps.Step
                    title="Create a Recipe File"
                    description={
                        <>
                            <Typography.Paragraph>
                                Define a YAML file defining the source from which you wish to extract metadata. This is
                                where you&apos;ll tell DataHub how to connect to your data source and configure the
                                metadata to be extracted.
                            </Typography.Paragraph>
                            <StepImage preview={false} height={300} src={recipeExample} />
                        </>
                    }
                />
                <Steps.Step
                    title="Run 'datahub ingest'"
                    description={
                        <>
                            <Typography.Paragraph>
                                Execute the datahub ingest command from your command line to ingest metadata into
                                DataHub.
                            </Typography.Paragraph>
                            <StepImage preview={false} height={52} src={ingestExample} />
                        </>
                    }
                />
            </Steps>
            <Typography.Paragraph>
                That&apos;s it! Once you&apos;ve ingested metadata, you can begin to search, document, tag, and assign
                ownership for your data assets.
            </Typography.Paragraph>
            <Typography.Title level={5}>Still have questions?</Typography.Title>
            <Typography.Paragraph>
                Join our <a href="https://slack.datahubproject.io/">Slack</a> to ask questions, provide feedback and
                more.
            </Typography.Paragraph>
        </StyledModal>
    );
};
