import { useCallback, useState } from 'react';

import CreateModuleModal from '@app/homeV3/addModule/modal/CreateModuleModal';
import { CreateModuleModalProps } from '@app/homeV3/addModule/types';
import { AddModuleHandlerInput } from '@app/homeV3/template/types';

interface Response {
    createModalProps: CreateModuleModalProps;
    onOpenModal: (input: AddModuleHandlerInput) => void;
    ModalComponent: React.FC<CreateModuleModalProps>;
}

export default function useCreateModuleModal(addModuleToTemplate: (input: AddModuleHandlerInput) => void): Response {
    const [input, setInput] = useState<AddModuleHandlerInput | undefined | null>();

    const onOpenModal = useCallback((addModuleInput: AddModuleHandlerInput) => {
        setInput(addModuleInput);
    }, []);

    const onCancel = useCallback(() => {
        setInput(null);
    }, []);

    const onCreate = useCallback(
        (addModuleInput: AddModuleHandlerInput) => {
            addModuleToTemplate(addModuleInput);
            onCancel();
        },
        [addModuleToTemplate, onCancel],
    );

    return {
        createModalProps: {
            input,
            onCancel,
            onCreate,
        },
        onOpenModal,
        ModalComponent: CreateModuleModal,
    };
}
