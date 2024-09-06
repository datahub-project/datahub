import { LogicalOperatorType, LogicalPredicate } from '@src/app/tests/builder/steps/definition/builder/types';
import { FormState } from '@src/types.generated';
import { Divider } from 'antd';
import React, { useContext, useState } from 'react';
import AddElement from './AddElement';
import AddQuestionModal from './AddQuestionModal';
import AddRecipients from './AddRecipients';
import LogicalFiltersBuilder from './filters/LogicalFiltersBuilder';
import { properties } from './filters/properties';
import { FormQuestion } from './formUtils';
import ManageFormContext from './ManageFormContext';
import QuestionsList from './QuestionsList';

const AddToForm = () => {
    const { formValues, setFormValues } = useContext(ManageFormContext);

    const [showQuestionModal, setShowQuestionModal] = useState<boolean>(false);
    const [currentQuestion, setCurrentQuestion] = useState<FormQuestion | undefined>();

    const handleFiltersChange = (updatedFilters?: LogicalPredicate) => {
        setFormValues({ ...formValues, filters: updatedFilters });
    };

    const addFilters = () => {
        setFormValues({
            ...formValues,
            filters: {
                operator: LogicalOperatorType.OR,
                operands: [],
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
                isButtonHidden={!!formValues.filters}
            />
            {formValues.filters && (
                <LogicalFiltersBuilder
                    filters={formValues.filters}
                    onChangeFilters={handleFiltersChange}
                    properties={properties}
                />
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
