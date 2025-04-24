import { Form } from 'antd';
import React, { useState } from 'react';

import { Input, Modal } from '@src/alchemy-components';

interface Props {
    onPropose: (description?: string) => void;
    onCancel: () => void;
    title?: string;
}

const ProposalDescriptionModal = ({ onPropose, onCancel, title }: Props) => {
    const [note, setNote] = useState('');

    return (
        <Modal
            title={title || 'Add Note'}
            onCancel={onCancel}
            buttons={[
                { text: 'Back', key: 'Back', variant: 'text', onClick: onCancel },
                {
                    text: 'Propose',
                    key: 'Propose',
                    variant: 'filled',
                    onClick: () => onPropose(note),
                    buttonDataTestId: 'add-note-propose-button',
                },
            ]}
        >
            <Form>
                <Input
                    label="Add Note"
                    placeholder="Note - optional"
                    value={note}
                    onChange={(e) => setNote(e.target.value)}
                />
            </Form>
        </Modal>
    );
};

export default ProposalDescriptionModal;
