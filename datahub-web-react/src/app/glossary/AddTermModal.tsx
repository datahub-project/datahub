import React, { useEffect, useState } from 'react';
import { Button, Form, Input, Modal, Select, Typography } from 'antd';
import { useApolloClient } from '@apollo/client';
import { EntityType } from '../../types.generated';
import { GetBrowseResultsDocument } from '../../graphql/browse.generated';

const { Option } = Select;

type Props = {
    visible: boolean;
    onClose: () => void;
    onCreate: (name: string, description: string) => void;
};

type Parent = {
    name: string;
    urn: string;
};

export const AddTermModal = ({ visible, onClose, onCreate }: Props) => {
    const [parents, setParents] = useState<Parent[]>([]);
    const client = useApolloClient();
    const entityType = EntityType.GlossaryTerm;

    const getTerms = (path: string[] = []) => {
        return client.query({
            query: GetBrowseResultsDocument,
            variables: {
                input: {
                    type: entityType,
                    path,
                    start: 0,
                    count: 1000,
                    filters: null,
                },
            },
        });
    };

    useEffect(() => {
        const fetchParents = async () => {
            const { data } = await getTerms();
            let fetchedParents = [];
            if (data && data.browse) {
                fetchedParents = data.browse.groups.map(({ name }) => ({
                    name,
                }));
            }
            setParents(fetchedParents);
        };

        fetchParents();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const [stagedName, setStagedName] = useState('');
    const [stagedDescription, setStagedDescription] = useState('');

    const onAddTerm = () => {
        onCreate('asd', 'asda');
    };
    return (
        <Modal
            title="Add Term"
            visible={visible}
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button id="addTermButton" onClick={onAddTerm} disabled={stagedName === ''}>
                        Add
                    </Button>
                </>
            }
        >
            <Form layout="vertical">
                <Form.Item name="name" label={<Typography.Text strong>Name</Typography.Text>}>
                    <Typography.Paragraph>Give your new Term a name. </Typography.Paragraph>
                    <Input
                        placeholder="A name for your term"
                        value={stagedName}
                        onChange={(event) => setStagedName(event.target.value)}
                    />
                </Form.Item>
                <Form.Item name="description" label={<Typography.Text strong>Description</Typography.Text>}>
                    <Typography.Paragraph>
                        An optional description for your new term. You can change this later.
                    </Typography.Paragraph>
                    <Input
                        placeholder="A description for your term"
                        value={stagedDescription}
                        onChange={(event) => setStagedDescription(event.target.value)}
                    />
                </Form.Item>
                <Form.Item name="parent" label={<Typography.Text strong>Parent</Typography.Text>}>
                    <Select placeholder="Select a parent" onChange={() => {}} allowClear>
                        {parents.map(({ name }) => (
                            <Option key={`parent-${name}`} value={name}>
                                {name}
                            </Option>
                        ))}
                    </Select>
                </Form.Item>
            </Form>
        </Modal>
    );
};
