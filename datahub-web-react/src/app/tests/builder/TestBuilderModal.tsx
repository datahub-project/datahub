import { Button, Form, Input, Modal, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import { jsonToYaml, yamlToJson } from '../../ingest/source/utils';
import { TestBuilderState } from '../types';
import { TestDefinitionForm } from './TestDefinitionBuilder';

const DEFAULT_TEST_JSON = `
{
  "on": {
    "dataset": [
      {
        "query": "dataPlatformInstance.platform",
        "condition": "EQUALS",
        "values": [
          "urn:li:dataPlatform:bigQuery"
        ]
      }
    ]
  },
  "rules": {
    "or": [
      {
        "query": "glossaryTerms.terms.glossaryTermInfo.parentNode",
        "condition": "EQUALS",
        "values": [
          "urn:li:glossaryNode:Category"
        ]
      },
      {
        "query": "container.container.glossaryTerms.terms.glossaryTermInfo.parentNode",
        "condition": "EQUALS",
        "values": [
          "urn:li:glossaryNode:Category"
        ]
      }
    ]
  }
}
`;

type Props = {
    initialState?: TestBuilderState;
    editing: boolean;
    visible: boolean;
    onSubmit?: (source: TestBuilderState, resetState: () => void) => void;
    onCancel?: () => void;
};

export const TestBuilderModal = ({ initialState, editing, visible, onSubmit, onCancel }: Props) => {
    const [testBuilderState, setTestBuilderState] = useState<TestBuilderState>(initialState || {});
    const [createButtonEnabled, setCreateButtonEnabled] = useState(false);
    const [form] = Form.useForm();

    console.log(editing);

    useEffect(() => {
        setTestBuilderState(initialState || {});
        form.resetFields();
    }, [initialState, form]);

    const setName = (name: string) => {
        setTestBuilderState({
            ...testBuilderState,
            name,
        });
    };

    const setCategory = (category: string) => {
        setTestBuilderState({
            ...testBuilderState,
            category,
        });
    };

    const setDescription = (description: string) => {
        setTestBuilderState({
            ...testBuilderState,
            description,
        });
    };

    const setDefinitionJson = (definitionJson: string | undefined) => {
        setTestBuilderState({
            ...testBuilderState,
            definition: {
                json: definitionJson,
            },
        });
    };

    const checkCreateButtonEnabled = (validYaml: boolean) => {
        const hasErrors = form.getFieldsError().some((field) => field.errors.length > 0);
        setCreateButtonEnabled(validYaml && !hasErrors);
    };

    const onChangeYaml = (definitionYaml: string) => {
        try {
            setDefinitionJson(yamlToJson(definitionYaml));
            checkCreateButtonEnabled(true);
        } catch (e) {
            setDefinitionJson(undefined);
            checkCreateButtonEnabled(false);
        }
    };

    return (
        <Modal
            width={800}
            style={{ top: 40 }}
            title={<Typography.Text>{editing ? 'Edit Test' : 'Create a new Test'}</Typography.Text>}
            visible={visible}
            onCancel={onCancel}
            footer={
                <>
                    <Button onClick={onCancel} type="text">
                        Cancel
                    </Button>
                    <Button
                        id="createTestButton"
                        onClick={() => onSubmit?.(testBuilderState, () => setTestBuilderState({}))}
                        disabled={!createButtonEnabled}
                    >
                        Done
                    </Button>
                </>
            }
        >
            <Form form={form} layout="vertical">
                <Form.Item required label={<Typography.Text strong>Name</Typography.Text>}>
                    <Typography.Paragraph>Give your test a name.</Typography.Paragraph>
                    <Input
                        placeholder="A name for your test"
                        value={testBuilderState.name}
                        onChange={(event) => setName(event.target.value)}
                    />
                </Form.Item>
                <Form.Item required label={<Typography.Text strong>Category</Typography.Text>}>
                    <Typography.Paragraph>The category of your test.</Typography.Paragraph>
                    <Input
                        placeholder="The category of your test"
                        value={testBuilderState.category}
                        onChange={(event) => setCategory(event.target.value)}
                    />
                </Form.Item>
                <Form.Item label={<Typography.Text strong>Description</Typography.Text>}>
                    <Typography.Paragraph>
                        An optional description to help keep track of your test.
                    </Typography.Paragraph>
                    <Input
                        placeholder="The description for your test"
                        value={testBuilderState.description || undefined}
                        onChange={(event) => setDescription(event.target.value)}
                    />
                </Form.Item>
                <TestDefinitionForm
                    onChange={onChangeYaml}
                    initialValue={jsonToYaml(initialState?.definition?.json || DEFAULT_TEST_JSON)}
                />
            </Form>
        </Modal>
    );
};
