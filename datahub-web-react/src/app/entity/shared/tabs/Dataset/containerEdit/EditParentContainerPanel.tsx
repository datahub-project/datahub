import { Button, Form } from 'antd';
import React from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { useBaseEntity } from '../../../EntityContext';
// import { Select } from 'antd';
import { SetParentContainer } from './SetParentContainer';

// import Link from 'antd/lib/typography/Link';

export const EditParentContainerPanel = () => {
    const dataset = useBaseEntity<GetDatasetQuery>();
    const platform = dataset?.dataset?.platform?.urn || '';
    const layout = {
        labelCol: {
            span: 6,
        },
        wrapperCol: {
            span: 14,
        },
    };
    const [form] = Form.useForm();
    const onFinish = (values) => {
        console.log(`values are ${values.parentContainerSelect}`);
    };
    return (
        <>
            <Form name="dynamic_item" {...layout} form={form} onFinish={onFinish}>
                <SetParentContainer platformType={platform} />
                <Button type="primary" htmlType="submit">
                    Submit
                </Button>
            </Form>
        </>
    );
};
