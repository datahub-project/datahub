import { Button, Text, TextArea } from '@src/alchemy-components';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType, FormPromptType, FormState } from '@src/types.generated';
import { Form, Input, Radio, Select } from 'antd';
import React, { useContext, useEffect, useState } from 'react';
import { v4 as uuidv4 } from 'uuid';
import { FormQuestion, questionTypes } from './formUtils';
import ManageFormContext from './ManageFormContext';
import { FieldLabel, FormFieldsContainer, ModalFooter, SelectOptionContainer, StyledModal } from './styledComponents';

interface Props {
    showQuestionModal: boolean;
    setShowQuestionModal: React.Dispatch<React.SetStateAction<boolean>>;
    setCurrentQuestion: React.Dispatch<React.SetStateAction<FormQuestion | undefined>>;
    question?: FormQuestion;
}

const AddQuestionModal = ({ showQuestionModal, setShowQuestionModal, setCurrentQuestion, question }: Props) => {
    const { formValues, setFormValues } = useContext(ManageFormContext);
    const [form] = Form.useForm();
    const [selectedType, setSelectedType] = useState<string | undefined>();

    const isFormDisabled = formValues.state !== FormState.Draft;

    const [structuredProperties, setStructuredProperties] = useState<
        | {
              label: string;
              value: string;
          }[]
        | undefined
    >();

    const inputs = {
        types: [EntityType.StructuredProperty],
        query: '*',
        start: 0,
        count: 100,
        searchFlags: { skipCache: true },
    };

    // Execute search
    const { data } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: inputs,
        },
    });

    useEffect(() => {
        const properties = data?.searchAcrossEntities?.searchResults.map((prop) => {
            return {
                label: (prop.entity as any).definition?.displayName,
                value: prop.entity.urn,
            };
        });
        setStructuredProperties(properties);
    }, [data]);

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
                    ind === index ? { ...ques, ...formData } : ques,
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

    const handleValuesChange = (values) => {
        const { type } = values;
        if (type) {
            setSelectedType(undefined);
            form.resetFields(['structuredPropertyParams', 'ownershipType']);
        }
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
            <Form form={form} onValuesChange={handleValuesChange} disabled={isFormDisabled}>
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
                        <Select placeholder="Select Question Type" onChange={(value) => setSelectedType(value)}>
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
                        selectedType === FormPromptType.FieldsStructuredProperty) && (
                        <>
                            <FieldLabel> Select Structured Property</FieldLabel>
                            <Form.Item
                                name={['structuredPropertyParams', 'urn']}
                                rules={[
                                    {
                                        required: true,
                                        message: 'Please select the structured property',
                                    },
                                ]}
                            >
                                <Select placeholder="Select Structured Property" options={structuredProperties} />
                            </Form.Item>
                        </>
                    )}
                    {selectedType === FormPromptType.Ownership && (
                        <>
                            <FieldLabel> Choose type of ownership</FieldLabel>
                            <Form.Item name="ownershipType">
                                <Radio.Group>
                                    <Radio value="single">Single owner</Radio>
                                    <Radio value="multiple">Multiple owners</Radio>
                                </Radio.Group>
                            </Form.Item>
                        </>
                    )}
                    <FieldLabel> Question</FieldLabel>
                    <Form.Item
                        name="title"
                        rules={[
                            {
                                required: true,
                                message: 'Please enter the question',
                            },
                        ]}
                    >
                        <Input placeholder="Add Question here" />
                    </Form.Item>
                    <Form.Item name="description">
                        <TextArea label="Description" placeholder="Add description here" isDisabled={isFormDisabled} />
                    </Form.Item>
                    <FieldLabel> Mandatory</FieldLabel>
                    <Form.Item name="required">
                        <Radio.Group defaultValue={false}>
                            <Radio value>Yes</Radio>
                            <Radio value={false}>No</Radio>
                        </Radio.Group>
                    </Form.Item>
                </FormFieldsContainer>
            </Form>
        </StyledModal>
    );
};

export default AddQuestionModal;
