import React from 'react';

type EditorProps = {
    content?: string;
    // eslint-disable-next-line react/no-unused-prop-types
    readOnly?: boolean;
    // eslint-disable-next-line react/no-unused-prop-types
    onChange?: (md: string) => void;
    // eslint-disable-next-line react/no-unused-prop-types
    className?: string;
};

/*
We need to mock the Editor component as it causes significant delays all over our
tests and causes CI failures due to timeouts. We test the Editor with tests for it
by itself as well as tests for the specific pieces of functionality it uses.
*/
export const Editor = (props: EditorProps) => {
    const { content } = props;

    return <div>{content}</div>;
};

export default Editor;
