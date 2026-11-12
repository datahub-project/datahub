import { ArrowRight } from '@phosphor-icons/react/dist/csr/ArrowRight';
import { Button } from 'antd';
import React from 'react';

type Props = {
    close: () => void;
};

export const CloseButton = ({ close }: Props) => {
    return (
        <Button type="text" onClick={close}>
            <ArrowRight />
        </Button>
    );
};
