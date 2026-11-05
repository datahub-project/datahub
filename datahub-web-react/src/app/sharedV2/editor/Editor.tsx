import type { NodeViewComponentProps } from '@remirror/react';
import React, { Suspense, forwardRef } from 'react';
import { ErrorBoundary } from 'react-error-boundary';

import { Editor as AlchemyEditor } from '@components/components/Editor';
import type { EditorMentionsPickerProps, EditorProps } from '@components/components/Editor/types';

import { useIsDocumentationFileUploadV1Enabled } from '@app/sharedV2/hooks/useIsDocumentationFileUploadV1Enabled';
import { lazyNamed } from '@app/sharedV2/lazyPage';

// Mentions UI pulls in search and entity rendering; load it with the first editable Editor.
const MentionsComponent = lazyNamed(
    () => import('@components/components/Editor/extensions/mentions/MentionsComponent'),
    'MentionsComponent',
);
const MentionsNodeView = lazyNamed(
    () => import('@components/components/Editor/extensions/mentions/MentionsNodeView'),
    'MentionsNodeView',
);

// A mentions chunk that fails to load leaves the editor without mentions UI instead of taking
// the page down.
function LazyMentionsPicker(props: EditorMentionsPickerProps) {
    return (
        <ErrorBoundary fallback={null}>
            <Suspense fallback={null}>
                <MentionsComponent {...props} />
            </Suspense>
        </ErrorBoundary>
    );
}

function LazyMentionNodeView(props: NodeViewComponentProps) {
    const placeholder = <span>{props.node.attrs.name}</span>;
    return (
        <ErrorBoundary fallback={placeholder}>
            <Suspense fallback={placeholder}>
                <MentionsNodeView {...props} />
            </Suspense>
        </ErrorBoundary>
    );
}

export type Props = Omit<EditorProps, 'mentionsPicker' | 'mentionNodeView' | 'imageUploadEnabled'>;

/** The alchemy Editor with DataHub mentions and the documentation file upload flag. */
export const Editor = forwardRef<unknown, Props>((props, ref) => {
    const imageUploadEnabled = useIsDocumentationFileUploadV1Enabled();

    return (
        <AlchemyEditor
            {...props}
            ref={ref}
            mentionsPicker={LazyMentionsPicker}
            mentionNodeView={LazyMentionNodeView}
            imageUploadEnabled={imageUploadEnabled}
        />
    );
});
Editor.displayName = 'Editor';
