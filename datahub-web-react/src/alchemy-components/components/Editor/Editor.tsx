import React, { Suspense, forwardRef } from 'react';

import type { EditorProps } from '@components/components/Editor/types';

const EditorImpl = React.lazy(() => import('./EditorImpl').then((m) => ({ default: m.Editor })));

// Thin shell that lazy-loads the remirror implementation so remirror is excluded
// from the initial bundle. Callers keep the same <Editor /> API including ref forwarding.
export const Editor = forwardRef<unknown, EditorProps>((props, ref) => (
    <Suspense fallback={null}>
        <EditorImpl {...props} ref={ref} />
    </Suspense>
));
Editor.displayName = 'Editor';
