import { Button, ButtonProps } from '@components';
import { Modal } from 'antd';
import React, { useState } from 'react';

interface Props extends ButtonProps {
    connections: {
        data: any;
        loading: boolean;
        error: any;
        refetch: () => void;
    };
    form?: any;
}

export const ConnectionCreateButton = ({ connections, form, ...props }: Props) => {
    const FormComponent = form;

    // Props
    const { refetch } = connections;

    // State
    const [isModalOpen, setIsModalOpen] = useState<boolean>(false);

    return (
        <>
            <Button icon={{ icon: 'Add' }} onClick={() => setIsModalOpen(true)} {...props}>
                Connection
            </Button>
            <Modal
                open={isModalOpen}
                onOk={() => setIsModalOpen(false)}
                onCancel={() => setIsModalOpen(false)}
                footer={null}
            >
                <FormComponent
                    disclosure={{
                        closeModal: () => {
                            setIsModalOpen(false);
                            refetch();
                        },
                    }}
                />
            </Modal>
        </>
    );
};
