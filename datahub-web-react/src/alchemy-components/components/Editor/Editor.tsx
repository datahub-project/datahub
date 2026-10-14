import React, { Suspense, forwardRef } from 'react';

import { ReadOnlyEditor } from '@components/components/Editor/ReadOnlyEditor';
import type { EditorProps } from '@components/components/Editor/types';

const EditorImpl = React.lazy(() => import('./EditorImpl').then((m) => ({ default: m.Editor })));

// Read-only documentation used to mount a full Remirror manager before any prose
// could paint, which made the ProseMirror paragraph the LCP element on home and
// dataset. Static HTML paints on the first commit; Remirror loads only for editing.
export const Editor = forwardRef<unknown, EditorProps>((props, ref) => {
    if (props.readOnly) {
        // No read-only caller passes a ref. The editable ref is the Remirror context,
        // which is not a div, so it cannot be forwarded here.
        return <ReadOnlyEditor {...props} />;
    }

    return (
        <Suspense fallback={null}>
            <EditorImpl {...props} ref={ref} />
        </Suspense>
    );
});
Editor.displayName = 'Editor';
