import { ArrowLeftOutlined, ArrowRightOutlined, SmileTwoTone } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';

import { FormView, useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { Flex } from '@app/entity/shared/entityForm/components';
import { pluralize } from '@app/shared/textUtil';

interface Props {
    handleViewRemaining?: (e: any) => void;
    closeModal?: () => void;
}

// ALL FORM TYPES: Some assets for prompt have response (clears query filters for current prompt)
const ByQuestionFinishRemainingAssets = ({ handleViewRemaining }: Omit<Props, 'closeModal'>) => {
    const {
        counts: {
            promptCounts: { numNotComplete },
        },
    } = useEntityFormContext();

    return (
        <Flex>
            <h4>{`Nice! You've set a response for all of the assets in this view.`}</h4>
            <p>
                {`Let's keep going! There are ${numNotComplete} ${pluralize(
                    numNotComplete,
                    'asset',
                )} still missing a response.`}
            </p>
            <Button type="primary" onClick={handleViewRemaining}>
                <ArrowRightOutlined /> View Remaining {numNotComplete} {pluralize(numNotComplete, 'Asset')}
            </Button>
        </Flex>
    );
};

// ALL FORM TYPES:
const ByQuestionContinueToNextQuestion = () => {
    const {
        prompt: { prompts, promptIndex, setSelectedPromptId },
    } = useEntityFormContext();

    function navigateRight() {
        if (prompts) {
            if (promptIndex === (prompts?.length || 0) - 1) {
                setSelectedPromptId(prompts?.[0].id);
            } else {
                setSelectedPromptId(prompts?.[promptIndex + 1].id);
            }
        }
    }

    return (
        <Flex>
            <h4>
                <SmileTwoTone twoToneColor="#11ADA0" />
                {`Hooray! You've set a response for all of your assets.`}
            </h4>
            <p>{`Let's keep that momentum going!`}</p>
            <Button type="primary" onClick={navigateRight}>
                <ArrowRightOutlined /> Continue to next question
            </Button>
        </Flex>
    );
};

const ByQuestionContinueToVerification = () => {
    const {
        form: { setFormView },
    } = useEntityFormContext();

    return (
        <Flex>
            <h4>
                <SmileTwoTone twoToneColor="#11ADA0" />
                {`Nice! You've set a response for everything on the final question.`}
            </h4>
            <p>Continue on to verify assets that are ready.</p>
            <Button type="primary" onClick={() => setFormView(FormView.BULK_VERIFY)}>
                <ArrowRightOutlined /> Continue to verification
            </Button>
        </Flex>
    );
};

const ByQuestionGoToPreviousQueston = () => {
    const {
        prompt: { prompts, promptIndex, setSelectedPromptId },
    } = useEntityFormContext();

    function navigateLeft() {
        if (prompts) {
            if (promptIndex === 0) {
                setSelectedPromptId(prompts?.[(prompts?.length || 0) - 1].id);
            } else {
                setSelectedPromptId(prompts?.[promptIndex - 1].id);
            }
        }
    }

    return (
        <Flex>
            <h4>
                <SmileTwoTone twoToneColor="#11ADA0" />
                {`Nice! You've set a response for everything on the final question.`}
            </h4>
            <p>{`Let's go back to finish any remaining questions!`}</p>
            <Button type="primary" onClick={navigateLeft}>
                <ArrowLeftOutlined /> Go to previous question
            </Button>
        </Flex>
    );
};

// NOT VERIFICATION FORM: All assets have response for all prompts
const ByQuestionCompleted = ({ closeModal }: Omit<Props, 'handleViewRemaining'>) => (
    <Flex>
        <h4>
            <SmileTwoTone twoToneColor="#11ADA0" />
            You Did It!
        </h4>
        <p>{`You've successfully completed the compliance tasks for all assets. Well done!`}</p>
        <Button type="primary" onClick={closeModal}>
            Close
        </Button>
    </Flex>
);

// VERIFICATION FORM: All ready assets are verified, but some assets need to be completed
const BulkVerifyFinishRemainingAssets = ({ handleViewRemaining }: { handleViewRemaining?: (e: any) => void }) => {
    const {
        counts: {
            verificationType: { verifyReady },
        },
    } = useEntityFormContext();

    return (
        <Flex>
            <h4>{`Nice! You've verified all of the assets in this view.`}</h4>
            <p>{`Let's keep going! There are ${verifyReady} ${pluralize(
                verifyReady,
                'asset',
            )} still ready for verification.`}</p>
            <Button type="primary" onClick={handleViewRemaining}>
                <ArrowRightOutlined /> View Remaining {verifyReady} {pluralize(verifyReady, 'Asset')}
            </Button>
        </Flex>
    );
};

// VERIFICATION FORM: Some assets are verified
const BulkVerifyReturnToQuestions = ({ returnToQuestions }: { returnToQuestions: (e: any) => void }) => (
    <Flex>
        <h4>
            <SmileTwoTone twoToneColor="#11ADA0" />
            {`Hooray! You've verified all of the assets that were ready.`}
        </h4>
        <p>{`Let's return to questions and add a response for any remaining assets!`}</p>
        <Button type="primary" onClick={returnToQuestions}>
            <ArrowLeftOutlined /> Return to Questions
        </Button>
    </Flex>
);

// VERIFICATION FORM: All assets are verified
const BulkVerifyCompleted = ({ closeModal }: Omit<Props, 'handleViewRemaining'>) => {
    return (
        <Flex>
            <h4>
                <SmileTwoTone twoToneColor="#11ADA0" />
                You Did It!
            </h4>
            <p>
                You&apos;ve successfully completed the documentation and verification requests for all assets. Well
                done!
            </p>
            <Button type="primary" onClick={closeModal}>
                Close
            </Button>
        </Flex>
    );
};

export const EmptyStates = ({ handleViewRemaining, closeModal }: Props) => {
    const {
        setShouldRefetch,
        search: { loading },
        states: { byQuestion, bulkVerify },
        form: { setFormView },
        entity: { setSelectedEntities },
    } = useEntityFormContext();

    if (loading) return null;

    const returnToQuestions = () => {
        setFormView(FormView.BY_QUESTION);
        setSelectedEntities([]);
        setShouldRefetch(true);
    };

    /*
     * By Question Flow
     */

    if (byQuestion.showCompleted)
        // not verification form type
        return <ByQuestionCompleted closeModal={closeModal} />;
    if (byQuestion.showFinishRemainingAssets)
        return <ByQuestionFinishRemainingAssets handleViewRemaining={handleViewRemaining} />;
    if (byQuestion.showContinueToVerification) return <ByQuestionContinueToVerification />;
    if (byQuestion.showGoToPreviousQuestion) return <ByQuestionGoToPreviousQueston />;
    if (byQuestion.showContinueToNextQuestion) return <ByQuestionContinueToNextQuestion />;

    /*
     * Bulk Verify Flow
     */

    if (bulkVerify.showReturnToQuestions) return <BulkVerifyReturnToQuestions returnToQuestions={returnToQuestions} />;
    if (bulkVerify.showFinishRemainingAssets)
        return <BulkVerifyFinishRemainingAssets handleViewRemaining={handleViewRemaining} />;
    if (bulkVerify.showCompleted) return <BulkVerifyCompleted closeModal={closeModal} />;

    return null;
};
