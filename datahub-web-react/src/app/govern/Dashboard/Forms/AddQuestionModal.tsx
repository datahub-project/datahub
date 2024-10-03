import { Button, Text } from '@src/alchemy-components';
import { WARNING_COLOR_HEX } from '@src/app/entityV2/shared/tabs/Incident/incidentUtils';
import { FormPrompt, FormPromptType, FormState } from '@src/types.generated';
import { Form, Select } from 'antd';
import React, { useContext, useEffect, useState } from 'react';
import { v4 as uuidv4 } from 'uuid';
import { questionTypes } from './formUtils';
import ManageFormContext from './ManageFormContext';
import CommonQuestionFields from './questionTypes/CommonQuestionFields';
import DomainsQuestion from './questionTypes/DomainQuestion';
import GlossaryTermsQuestion from './questionTypes/GlossaryTermsQuestion';
import OwnershipQuestion from './questionTypes/OwnershipQuestion';
import RequiredField from './questionTypes/RequiredField';
import StructuredPropertyQuestion from './questionTypes/StructuredPropertyQuestion';
import {
    CustomDropdown,
    FieldLabel,
    FooterButtonsContainer,
    FormFieldsContainer,
    ModalFooter,
    SelectOptionContainer,
    StyledExclamationOutlined,
    StyledModal,
    StyledSelect,
    WarningWrapper,
} from './styledComponents';

interface Props {
    showQuestionModal: boolean;
    setShowQuestionModal: React.Dispatch<React.SetStateAction<boolean>>;
    setCurrentQuestion: React.Dispatch<React.SetStateAction<FormPrompt | undefined>>;
    question?: FormPrompt;
}

const DEPENDENT_FIELDS = ['structuredPropertyParams', 'ownershipParams', 'glossaryTermsParams', 'domainParams'];

const AddQuestionModal = ({ showQuestionModal, setShowQuestionModal, setCurrentQuestion, question }: Props) => {
    const { formValues, setFormValues } = useContext(ManageFormContext);
    const [form] = Form.useForm();
    const [selectedType, setSelectedType] = useState<string | undefined>();

    const isFormDisabled = formValues.state !== FormState.Draft;

    const quesType = form.getFieldValue('type') || '';
    const required = question ? form.getFieldValue('required') : !quesType.startsWith('FIELD');
    const [isRequired, setIsRequired] = useState<boolean>(required);

    useEffect(() => {
        setIsRequired(required);
    }, [required]);

    useEffect(() => {
        form.setFieldsValue(question || {});
        setSelectedType(question?.type);
    }, [form, question]);

    useEffect(() => {
        const questionObject = questionTypes.find((type) => type.value === selectedType);
        form.setFieldsValue({
            title: questionObject?.defaultTitle,
            description: questionObject?.defaultDescription,
        });
    }, [selectedType, form]);

    const handleCreateOrUpdateQuestion = () => {
        const formData = form.getFieldsValue(true);
        const questions = formValues.questions || [];

        form.validateFields().then(() => {
            // Editing an existing question
            if (question) {
                const updatedQuestions = questions?.map((ques) =>
                    ques.id === question.id ? { id: ques.id, ...formData } : ques,
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
                    <RequiredField form={form} isRequired={isRequired} setIsRequired={setIsRequired} />
                    <FooterButtonsContainer>
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
                    </FooterButtonsContainer>
                </ModalFooter>
            }
            destroyOnClose
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
                        <StyledSelect
                            placeholder="Select Question Type"
                            onChange={(value: any) => {
                                resetDependentFields();
                                setSelectedType(value);
                                form.setFieldsValue(
                                    question
                                        ? { ...question, type: value, required: !(value as string).startsWith('FIELD') }
                                        : {
                                              type: value,
                                              required: !(value as string).startsWith('FIELD'),
                                          },
                                );
                            }}
                            dropdownRender={(menu) => <CustomDropdown>{menu}</CustomDropdown>}
                        >
                            {questionTypes.map((questionType) => {
                                return (
                                    <Select.Option key={questionType.value} value={questionType.value}>
                                        <SelectOptionContainer>
                                            <Text color="gray" weight="medium" size="md">
                                                {questionType.label}
                                            </Text>
                                            <Text color="gray" weight="normal" size="sm">
                                                {questionType.description}
                                            </Text>
                                        </SelectOptionContainer>
                                    </Select.Option>
                                );
                            })}
                        </StyledSelect>
                    </Form.Item>

                    {(selectedType === FormPromptType.StructuredProperty ||
                        selectedType === FormPromptType.FieldsStructuredProperty) && <StructuredPropertyQuestion />}
                    {selectedType && <CommonQuestionFields isFormDisabled={isFormDisabled} />}
                    {selectedType === FormPromptType.Ownership && <OwnershipQuestion />}
                    {selectedType === FormPromptType.GlossaryTerms && <GlossaryTermsQuestion />}
                    {selectedType === FormPromptType.FieldsGlossaryTerms && <GlossaryTermsQuestion />}
                    {selectedType === FormPromptType.Domain && <DomainsQuestion />}

                    {isRequired && quesType.startsWith('FIELD') && (
                        <WarningWrapper>
                            <StyledExclamationOutlined color={WARNING_COLOR_HEX} />
                            <span>
                                <strong>Are you sure?</strong> All columns will need an anwer to this question
                                individually to complete the form.
                            </span>
                        </WarningWrapper>
                    )}
                </FormFieldsContainer>
            </Form>
        </StyledModal>
    );
};

export default AddQuestionModal;
