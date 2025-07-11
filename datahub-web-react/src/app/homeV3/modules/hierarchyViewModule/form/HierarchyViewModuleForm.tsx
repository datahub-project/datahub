import React from 'react';
import { Form, FormInstance } from "antd";
import { Input } from '@components';
import FormTabs from './FormTabs';
import AssetsSection from './sections/AssetsSection';
import { useForm } from 'antd/es/form/Form';

interface HierarchyViewModuleFormProps {
    name: string;
    description?: string;
}

function BaseModuleFields() {
    return <>
        <Form.Item
            name="name"
            rules={[
                {
                    required: true,
                    message: 'Enter a widget name.',
                },
                { whitespace: true },
                { min: 1, max: 50 },
            ]}
            hasFeedback
        >
            <Input placeholder="Choose a name for your widget" label="Name" />
        </Form.Item>

        {/* TODO: ask if it should be supported */}
        {/* <Form.Item
                name="description"
                rules={[
                    {
                        required: true,
                        message: 'Enter a widget name.',
                    },
                    { whitespace: true },
                    { min: 1, max: 50 },
                ]}
                hasFeedback
            >
                <Input placeholder="Help others understand what this collection contains..." label="Description (Optional)" type='number'/>
            </Form.Item> */}

    </>
}

interface Props {
    form: FormInstance<HierarchyViewModuleFormProps>;
}

export default function HierarchyViewModuleForm({form}: Props) {
    return (
        <>
            <BaseModuleFields />

            <AssetsSection />
        </>
    );
}