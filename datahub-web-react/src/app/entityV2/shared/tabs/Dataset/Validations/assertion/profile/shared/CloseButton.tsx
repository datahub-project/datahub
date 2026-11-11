import { Button } from '@components';
import { ArrowRight } from '@phosphor-icons/react/dist/csr/ArrowRight';
import React from 'react';

type Props = {
    close: () => void;
};

export const CloseButton = ({ close }: Props) => {
    return (
        <Button variant="text" color="gray" onClick={close}>
            <ArrowRight />
        </Button>
    );
};
