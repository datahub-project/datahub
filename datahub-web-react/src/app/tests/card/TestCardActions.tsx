import React from 'react';
import styled from 'styled-components';
import { Test } from '../../../types.generated';
import TestCardActionMenu from './TestCardActionMenu';
import TestCardEditButton from './TestCardEditButton';

const Container = styled.div``;

type Props = {
    test: Test;
    showEdit: boolean;
    showDelete: boolean;
    onClickEdit: () => void;
    onClickDelete: () => void;
    index: number;
};

export const TestCardActions = ({ test, showEdit, showDelete, onClickEdit, onClickDelete, index }: Props) => {
    return (
        <Container>
            {showEdit && <TestCardEditButton onClickEdit={onClickEdit} index={index} />}
            {showDelete && test.urn && (
                <TestCardActionMenu onClickDelete={onClickDelete} index={index} urn={test.urn} />
            )}
        </Container>
    );
};
