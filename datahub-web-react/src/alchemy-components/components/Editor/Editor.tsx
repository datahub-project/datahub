import React, { Suspense, forwardRef } from 'react';

import { ReadOnlyEditor } from '@components/components/Editor/ReadOnlyEditor';
import type { EditorProps } from '@components/components/Editor/types';

const EditorImpl = React.lazy(() => import('./EditorImpl').then((m) => ({ default: m.Editor })));

// Read-only documentation used to mount a full Remirror manager before any prose
// could paint, which made the ProseMirror paragraph the LCP element on home and
// dataset. Static HTML paints on the first commit; Remirror loads only for editing.
export const Editor = forwardRef<unknown, EditorProps>((props, ref) => {
    if (props.readOnly) {
        // The editable ref is the Remirror context. The read-only ref is the container div.
        return <ReadOnlyEditor {...props} ref={ref as React.Ref<HTMLDivElement>} />;
    }

    return (
        <Suspense fallback={null}>
            <EditorImpl {...props} ref={ref} />
        </Suspense>
    );
});
Editor.displayName = 'Editor';
