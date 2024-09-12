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
        let orFilters: AndFilterInput[] = [{ and: [{ field: 'NULL', values: ['NULL'] }] }];
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

    return (
        <>
            <AddElement
                heading="Add Questions"
                description="Add some questions"
                buttonLabel="Add Questions"
                buttonOnClick={() => setShowQuestionModal(true)}
                isButtonDisabled={formValues.state !== FormState.Draft}
            />
            <QuestionsList setShowQuestionModal={setShowQuestionModal} setCurrentQuestion={setCurrentQuestion} />

            <Divider />
            <AddElement
                heading="Assign Assets"
                description="Assign the Assets for which you want to collect the data"
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
