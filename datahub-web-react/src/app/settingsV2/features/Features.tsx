import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';
import { v4 as uuidv4 } from 'uuid';

import { Feature, FeatureType } from '@app/settingsV2/features/Feature';
import {
    useGetDocPropagationSettings,
    useUpdateDocPropagationSettings,
} from '@app/settingsV2/features/useDocPropagationSettings';
import { PageTitle } from '@src/alchemy-components';

const Page = styled.div`
    display: flex;
    padding: 16px 20px 16px 20px;
    width: 100%;
`;

const SourceContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 24px;
    width: 100%;
`;
const Container = styled.div`
    width: 100%;
`;

export const Features = () => {
    const { t } = useTranslation('settings.features');

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
            title: t('docPropagation.title'),
            description: t('docPropagation.description'),
            settings: [
                {
                    key: uuidv4(),
                    title: t('docPropagation.rollbackTitle'),
                    isAvailable: false,
                    buttonText: t('docPropagation.rollbackButton'),
                },
                {
                    key: uuidv4(),
                    title: t('docPropagation.backfillTitle'),
                    isAvailable: false,
                    buttonText: t('docPropagation.backfillButton'),
                },
            ],
            options: [
                {
                    key: uuidv4(),
                    title: t('docPropagation.columnLevelTitle'),
                    description: t('docPropagation.columnLevelDescription'),
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
                    title: t('docPropagation.assetLevelTitle'),
                    description: t('docPropagation.assetLevelDescription'),
                    checked: false,
                    onChange: (_: boolean) => null,
                    isAvailable: true,
                    isDisabled: true,
                    disabledMessage: t('docPropagation.comingSoon'),
                },
            ],
            isNew: true,
            learnMoreLink:
                'https://docs.datahub.com/docs/automations/docs-propagation?utm_source=datahub_core&utm_medium=docs&utm_campaign=features',
        },
    ];

    // Render
    return (
        <Page>
            <SourceContainer>
                <Container>
                    <PageTitle title={t('pageTitle')} subTitle={t('pageSubTitle')} />
                </Container>
                {features.map((feature) => (
                    <Feature {...feature} />
                ))}
            </SourceContainer>
        </Page>
    );
};
