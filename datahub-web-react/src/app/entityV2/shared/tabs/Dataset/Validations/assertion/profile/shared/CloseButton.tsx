import { Button } from 'antd';
import React from 'react';
import { ArrowRight } from '@phosphor-icons/react/dist/csr/ArrowRight';

type Props = {
    close: () => void;
};

export const CloseButton = ({ close }: Props) => {
    return (
        <Button type="text" onClick={close}>
            <ArrowRight  />
        </Button>
    );
};
