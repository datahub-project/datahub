import { LogicalOperatorType, LogicalPredicate } from '@src/app/tests/builder/steps/definition/builder/types';
import { AndFilterInput, FormPrompt, FormState } from '@src/types.generated';
import { convertLogicalPredicateToOrFilters } from '@src/app/tests/builder/steps/definition/builder/utils';
import { Divider } from 'antd';
import React, { useContext, useState } from 'react';
import AddElement from './AddElement';
import AddQuestionModal from './AddQuestionModal';
import AddRecipients from './AddRecipients';
import LogicalFiltersBuilder from './filters/LogicalFiltersBuilder';
import { properties } from './filters/properties';
import ManageFormContext from './ManageFormContext';
import QuestionsList from './QuestionsList';
import AssetReviewModal from './AssetReviewModal';

const AddToForm = () => {
    const { formValues, setFormValues } = useContext(ManageFormContext);

    const [showQuestionModal, setShowQuestionModal] = useState<boolean>(false);
    const [currentQuestion, setCurrentQuestion] = useState<FormPrompt | undefined>();

    const handleFiltersChange = (updatedPredicate?: LogicalPredicate) => {
        // create null filter so no entities match this by default
        let orFilters: AndFilterInput[] = [{ and: [{ field: 'urn', values: ['urn:li:fakeUrnWithNoMatches'] }] }];
        if (updatedPredicate && updatedPredicate.operands.length > 0) {
            // if there are filters, convert them to orFilters format
            orFilters = convertLogicalPredicateToOrFilters(updatedPredicate);
        }
        setFormValues({ ...formValues, assets: { logicalPredicate: updatedPredicate, orFilters } });
    };

    const addFilters = () => {
        setFormValues({
            ...formValues,
            assets: {
                logicalPredicate: {
                    operator: LogicalOperatorType.OR,
                    operands: [],
                },
            },
        });
    };

    const isAddQuestionDisabled = formValues.state !== FormState.Draft;

    return (
        <>
            <AddElement
                heading="Add Questions"
                description="Create the requirements, or questions, that must be provided for each assigned asset."
                buttonLabel="Add Question"
                buttonOnClick={() => setShowQuestionModal(true)}
                isButtonDisabled={isAddQuestionDisabled}
                buttonTooltip={
                    isAddQuestionDisabled
                        ? 'New questions cannot be added once a form has been published. To add new questions create a new compliance form.'
                        : undefined
                }
            />
            <QuestionsList setShowQuestionModal={setShowQuestionModal} setCurrentQuestion={setCurrentQuestion} />

            <Divider />
            <AddElement
                heading="Assign Assets"
                description="Select the assets to assign this form to"
                buttonLabel="Add Assets"
                buttonOnClick={addFilters}
                isButtonHidden={!!formValues.assets?.logicalPredicate}
            />
            {formValues.assets?.logicalPredicate && (
                <>
                    <LogicalFiltersBuilder
                        filters={formValues.assets?.logicalPredicate}
                        onChangeFilters={handleFiltersChange}
                        properties={properties}
                    />
                    <AssetReviewModal />
                </>
            )}
            <Divider />

            <AddRecipients />

            <AddQuestionModal
                showQuestionModal={showQuestionModal}
                setShowQuestionModal={setShowQuestionModal}
                question={currentQuestion}
                setCurrentQuestion={setCurrentQuestion}
            />
        </>
    );
};

export default AddToForm;
