import { SmileOutlined } from '@ant-design/icons';
import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
import { OnboardingStep } from '../OnboardingStep';
import BulkTypeComparions from '../../../images/bulk-form-type-comparison.svg';

const DiagramHeader = styled.div`
    display: flex;
    justify-content: center;
    margin: 16px 0 4px 0;
`;

const AssetCompletionHeader = styled.div`
    font-size: 20px;
    font-weight: normal;
`;

const ByAssetWrapper = styled.span`
    margin-left: 10px;
    font-size: 14px;
`;

const ByQuestionWrapper = styled.span`
    margin-left: 80px;
    font-size: 14px;
`;

const StyledSmile = styled(SmileOutlined)`
    color: ${(props) => props.theme.styles['primary-color']};
    margin-right: 4px;
`;

export const WELCOME_TO_BULK_BY_ENTITY_ID = 'welcome-to-bulk-by-entity';
export const FORM_QUESTION_VIEW_BUTTON = 'form-question-view-button';
export const FORM_ASSET_COMPLETION = 'form-asset-completion';
export const WELCOME_TO_BULK_BY_QUESTION_ID = 'welcome-to-bulk-by-question';
export const FORM_ASSETS_ASSIGNED_ID = 'form-assets-assigned';
export const FORM_FILTER_AND_BROWSE_ID = 'form-filter-and-browse';
export const FORM_ANSWER_IN_BULK_ID = 'form-answer-in-bulk';
export const FORM_BULK_VERIFY_INTRO_ID = 'form-bulk-verify-intro';
export const FORM_CHECK_RESPONSES_ID = 'form-check-responses';
export const FORM_BULK_VERIFY_ID = 'form-bulk-verify';

export const FormOnboardingConfig: OnboardingStep[] = [
    {
        id: WELCOME_TO_BULK_BY_ENTITY_ID,
        selector: `#${WELCOME_TO_BULK_BY_ENTITY_ID}`,
        title: 'Let’s complete your documentation requests!',
        style: { width: '520px', maxWidth: '520px' },
        content: (
            <Typography.Paragraph>
                Here you can easily respond to all documentation requests efficiently. We’ll track your progress and
                move you seamlessly through all your requests.
                <br />
                Let’s get started completing the needs for this form.
            </Typography.Paragraph>
        ),
    },
    {
        id: FORM_QUESTION_VIEW_BUTTON,
        selector: `#${FORM_QUESTION_VIEW_BUTTON}`,
        title: "Switch to the 'Complete by Question' view.",
        style: { width: '520px', maxWidth: '520px' },
        content: (
            <Typography.Paragraph>
                If an answer fits multiple assets, this view lets you tackle questions across different assets at once,
                making documentation even faster and more efficient.
            </Typography.Paragraph>
        ),
    },
    {
        id: FORM_ASSET_COMPLETION,
        selector: `#${FORM_ASSET_COMPLETION}`,
        isActionStep: true,
        title: (
            <AssetCompletionHeader>
                <StyledSmile /> Congratulations, You’ve Completed 1 Asset!
            </AssetCompletionHeader>
        ),
        style: { width: '640px', maxWidth: '640px' },
        content: (
            <Typography.Paragraph>
                Now that you’ve completed one asset, try switching to the ‘Complete by Question’ view. If an answer fits
                multiple assets, this view lets you tackle questions across different assets at once, making
                documentation even faster and more efficient.
                <DiagramHeader>
                    <ByAssetWrapper>By Asset</ByAssetWrapper>
                    <ByQuestionWrapper>By Question</ByQuestionWrapper>
                </DiagramHeader>
                <img src={BulkTypeComparions} alt="bulk form type comparions" style={{ width: '100%' }} />
            </Typography.Paragraph>
        ),
    },
    {
        id: WELCOME_TO_BULK_BY_QUESTION_ID,
        selector: `#${WELCOME_TO_BULK_BY_QUESTION_ID}`,
        title: "Welcome to the 'Complete by Question' view!",
        style: { width: '520px', maxWidth: '520px' },
        content: (
            <Typography.Paragraph>
                Here, you can easily provide the same response for multiple assets at once for a faster documenting
                experience.
            </Typography.Paragraph>
        ),
    },
    {
        id: FORM_ASSETS_ASSIGNED_ID,
        selector: `#${FORM_ASSETS_ASSIGNED_ID}`,
        title: 'Focus on only the assets that require your attention',
        style: { width: '520px', maxWidth: '520px' },
        content: (
            <Typography.Paragraph>
                In this view, we’ve simplified your workflow by only showing assets that require documentation from you.
            </Typography.Paragraph>
        ),
    },
    {
        id: FORM_FILTER_AND_BROWSE_ID,
        selector: `#${FORM_FILTER_AND_BROWSE_ID}`,
        title: 'Filter and Browse to Select the Specific Assets',
        style: { width: '520px', maxWidth: '520px' },
        content: (
            <Typography.Paragraph>
                Filter by type, terms, or browse by platform, database and schemas to select only the assets that you’d
                like to set the response for.
            </Typography.Paragraph>
        ),
    },
    {
        id: FORM_ANSWER_IN_BULK_ID,
        selector: `#${FORM_ANSWER_IN_BULK_ID}`,
        title: 'Answer in Bulk',
        style: { width: '520px', maxWidth: '520px' },
        content: (
            <Typography.Paragraph>
                After selecting your assets, set a collective response and start answering for groups of 1,000 assets at
                a time.
            </Typography.Paragraph>
        ),
    },
    {
        id: FORM_BULK_VERIFY_INTRO_ID,
        selector: `#${FORM_BULK_VERIFY_INTRO_ID}`,
        title: 'Streamline Verification in Bulk!',
        style: { width: '520px', maxWidth: '520px' },
        content: (
            <Typography.Paragraph>
                Here you can quickly review responses for a few datasets, ensuring accuracy. When you&apos;re ready,
                proceed to verify all assets at once, simplifying the entire verification process.
            </Typography.Paragraph>
        ),
    },
    {
        id: FORM_CHECK_RESPONSES_ID,
        selector: `#${FORM_CHECK_RESPONSES_ID}`,
        title: 'Check Responses',
        style: { width: '520px', maxWidth: '520px' },
        content: (
            <Typography.Paragraph>
                Click on &quot;View Responses&quot; to easily spot-check your responses before the final Verification
                step.
            </Typography.Paragraph>
        ),
    },
    {
        id: FORM_BULK_VERIFY_ID,
        selector: `#${FORM_BULK_VERIFY_ID}`,
        title: 'Bulk Verify Assets',
        style: { width: '520px', maxWidth: '520px' },
        content: (
            <Typography.Paragraph>
                Once you&apos;re confident in your responses, verify up to 1,000 assets at a time for this form with a
                click of a button.
            </Typography.Paragraph>
        ),
    },
];
