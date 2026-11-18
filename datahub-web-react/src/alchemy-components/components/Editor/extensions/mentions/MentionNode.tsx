import { NodeViewComponentProps } from '@remirror/react';
import React, { ComponentType } from 'react';

import type { EditorMentionViewProps } from '@components/components/Editor/types';

type Props = NodeViewComponentProps & {
    /** The host app's mention view. */
    view?: ComponentType<EditorMentionViewProps>;
};

/** Renders a mention with the host app's view, or as plain `@name` without one. */
export function MentionNode({ view: View, node }: Props) {
    const { urn, name } = node.attrs;
    if (View) return <View urn={urn} name={name} />;

    return <span className="mentions">@{name}</span>;
}
