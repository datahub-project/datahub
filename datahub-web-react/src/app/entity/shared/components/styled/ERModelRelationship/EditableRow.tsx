import React from 'react';
import { Form } from 'antd';
import { EditableContext } from './ERModelRelationUtils';

interface EditableRowProps {
    index: number;
}
export const EditableRow: React.FC<EditableRowProps> = ({ ...propsAt }) => {
    const [form] = Form.useForm();
    return (
        <Form form={form} component={false}>
            <EditableContext.Provider value={form}>
                <tr {...propsAt} />
            </EditableContext.Provider>
        </Form>
    );
};
