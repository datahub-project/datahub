import React, { Suspense } from 'react';

import type { EditorProps } from '@components/components/Editor/types';

const EditorImpl = React.lazy(() => import('./EditorImpl').then((m) => ({ default: m.Editor })));

// Thin shell that lazy-loads the remirror implementation so remirror is excluded
// from the initial bundle. Callers keep the same <Editor /> API.
export const Editor = (props: EditorProps) => (
    <Suspense fallback={null}>
        <EditorImpl {...props} />
    </Suspense>
);
