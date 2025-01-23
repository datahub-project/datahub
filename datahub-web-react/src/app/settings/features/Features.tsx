import React from 'react';

import styled from 'styled-components';

import { Divider, Typography } from 'antd';
import { v4 as uuidv4 } from 'uuid';

import { Feature, FeatureType } from './Feature';

import { useGetDocPropagationSettings, useUpdateDocPropagationSettings } from './useDocPropagationSettings';

const Page = styled.div`
    width: 100%;
    display: flex;
    justify-content: center;
`;

const SourceContainer = styled.div`
    width: 80%;
    padding-top: 20px;
    padding-right: 40px;
    padding-left: 40px;
`;
const Container = styled.div`
    padding-top: 0px;
`;

const Title = styled(Typography.Title)`
    && {
        margin-bottom: 8px;
    }
`;

export const Features = () => {
    /*
     * Note: When adding new features, make sure to update the features array below
     * and create a hook file for the new feature in the same directory
     */

    // Hooks to get and update the document propagation settings
    const { isColPropagateChecked, setIsColPropagateChecked } = useGetDocPropagationSettings();
    const { updateDocPropagation } = useUpdateDocPropagationSettings();

    // Features to display
    const features: FeatureType[] = [
        {
            key: uuidv4(),
            title: 'Documentation Propagation',
            description: 'Automatically propagate documentation from upstream to downstream columns and assets.',
            settings: [
                {
                    key: uuidv4(),
                    title: 'Rollback Propagation Changes',
                    isAvailable: false,
                    buttonText: 'Rollback',
                },
                {
                    key: uuidv4(),
                    title: 'Backfill existing documentation from upstream to downstream columns/assets',
                    isAvailable: false,
                    buttonText: 'Initialize',
                },
            ],
            options: [
                {
                    key: uuidv4(),
                    title: 'Column Level Propagation',
                    description:
                        'Propagate new documentation from upstream to downstream columns based on column-level lineage relationships.',
                    isAvailable: true,
                    checked: isColPropagateChecked,
                    onChange: (checked: boolean) => {
                        setIsColPropagateChecked(checked);
                        updateDocPropagation(checked);
                    },
                    isDisabled: false,
                    disabledMessage: undefined,
                },
                {
                    key: uuidv4(),
                    title: 'Asset Level Propagation',
                    description:
                        'Propagate new documentation from upstream to downstream assets based on data lineage relationships.',
                    checked: false,
                    onChange: (_: boolean) => null,
                    isAvailable: true,
                    isDisabled: true,
                    disabledMessage: 'Coming soon!',
                },
            ],
            isNew: true,
            learnMoreLink:
                'https://datahubproject.io/docs/automations/docs-propagation?utm_source=datahub_core&utm_medium=docs&utm_campaign=features',
        },
    ];

    // Render
    return (
        <Page>
            <SourceContainer>
                <Container>
                    <div>
                        <Title level={2}>Features</Title>
                        <Typography.Paragraph type="secondary">
                            Explore and configure specific features
                        </Typography.Paragraph>
                    </div>
                </Container>
                <Divider />
                {features.map((feature) => (
                    <Feature {...feature} />
                ))}
            </SourceContainer>
        </Page>
    );
};
