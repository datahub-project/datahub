import { Button, Text } from '@src/alchemy-components';
import { FormPromptType, FormState } from '@src/types.generated';
import { Form, Select } from 'antd';
import React, { useContext, useEffect, useState } from 'react';
import { v4 as uuidv4 } from 'uuid';
import { FormQuestion, questionTypes } from './formUtils';
import ManageFormContext from './ManageFormContext';
import CommonQuestionFields from './questionTypes/CommonQuestionFields';
import OwnershipQuestion from './questionTypes/OwnershipQuestion';
import StructuredPropertyQuestion from './questionTypes/StructuredPropertyQuestion';
import { FieldLabel, FormFieldsContainer, ModalFooter, SelectOptionContainer, StyledModal } from './styledComponents';
import GlossaryTermsSelector from './questionTypes/GlossaryTermsSelector';

interface Props {
    showQuestionModal: boolean;
    setShowQuestionModal: React.Dispatch<React.SetStateAction<boolean>>;
    setCurrentQuestion: React.Dispatch<React.SetStateAction<FormQuestion | undefined>>;
    question?: FormQuestion;
}

const DEPENDENT_FIELDS = ['structuredPropertyParams', 'ownershipParams'];

const AddQuestionModal = ({ showQuestionModal, setShowQuestionModal, setCurrentQuestion, question }: Props) => {
    const { formValues, setFormValues } = useContext(ManageFormContext);
    const [form] = Form.useForm();
    const [selectedType, setSelectedType] = useState<string | undefined>();

    const isFormDisabled = formValues.state !== FormState.Draft;

    useEffect(() => {
        form.setFieldsValue(
            question || {
                required: false,
            },
        );
        setSelectedType(question?.type);
    }, [form, question]);

    const handleCreateOrUpdateQuestion = () => {
        const formData = form.getFieldsValue();
        const questions = formValues.questions || [];

        form.validateFields().then(() => {
            // Editing an existing question
            if (question) {
                const index = questions?.indexOf(question);
                const updatedQuestions = questions?.map((ques, ind) =>
                    ind === index ? { id: ques.id, ...formData } : ques,
                );
                setFormValues({ ...formValues, questions: updatedQuestions });
            } else {
                // Adding a new question
                const newQuestion = {
                    ...formData,
                    id: uuidv4(),
                };
                setFormValues({ ...formValues, questions: [...questions, newQuestion] });
            }

            setShowQuestionModal(false);
            form.resetFields();
            setCurrentQuestion(undefined);
            setSelectedType(undefined);
        });
    };

    const handleModalClose = () => {
        setShowQuestionModal(false);
        form.resetFields();
        setCurrentQuestion(undefined);
        setSelectedType(undefined);
    };

    const resetDependentFields = () => {
        const fieldValues = form.getFieldsValue();
        DEPENDENT_FIELDS.map((field) => delete fieldValues[field]);
        form.setFieldsValue(fieldValues);
    };

    const getModalTitle = () => {
        if (isFormDisabled) return 'View Question';
        if (question) return 'Edit Question';
        return 'Add Question';
    };

    return (
        <StyledModal
            title={
                <Text color="gray" size="lg" weight="bold">
                    {getModalTitle()}
                </Text>
            }
            open={showQuestionModal}
            onCancel={handleModalClose}
            footer={
                <ModalFooter>
                    {isFormDisabled ? (
                        <Button onClick={handleModalClose}>Close</Button>
                    ) : (
                        <>
                            <Button variant="text" onClick={handleModalClose}>
                                Cancel
                            </Button>
                            <Button onClick={handleCreateOrUpdateQuestion}>{question ? 'Update' : 'Create'}</Button>
                        </>
                    )}
                </ModalFooter>
            }
        >
            <Form form={form} disabled={isFormDisabled}>
                <FormFieldsContainer>
                    <FieldLabel> Type</FieldLabel>
                    <Form.Item
                        name="type"
                        rules={[
                            {
                                required: true,
                                message: 'Please select the question type',
                            },
                        ]}
                    >
                        <Select
                            placeholder="Select Question Type"
                            onChange={(value) => {
                                resetDependentFields();
                                setSelectedType(value);
                            }}
                        >
                            {questionTypes.map((questionType) => {
                                return (
                                    <Select.Option key={questionType.value} value={questionType.value}>
                                        <SelectOptionContainer>
                                            <Text color="gray" weight="semiBold" size="md">
                                                {questionType.label}
                                            </Text>
                                            <Text color="gray" size="sm">
                                                {questionType.description}
                                            </Text>
                                        </SelectOptionContainer>
                                    </Select.Option>
                                );
                            })}
                        </Select>
                    </Form.Item>

                    {(selectedType === FormPromptType.StructuredProperty ||
                        selectedType === FormPromptType.FieldsStructuredProperty) && <StructuredPropertyQuestion />}

                    {selectedType === FormPromptType.Ownership && <OwnershipQuestion />}
                    {selectedType === FormPromptType.GlossaryTerms && <GlossaryTermsSelector />}
                    {selectedType === FormPromptType.FieldsGlossaryTerms && <GlossaryTermsSelector />}

                    {selectedType && <CommonQuestionFields isFormDisabled={isFormDisabled} />}
                </FormFieldsContainer>
            </Form>
        </StyledModal>
    );
};

export default AddQuestionModal;
