import { NodeViewComponentProps } from '@remirror/react';
import React, { ComponentType } from 'react';

type Props = NodeViewComponentProps & {
    /** The host app's node view. */
    view?: ComponentType<NodeViewComponentProps>;
};

/** Renders a mention with the host app's node view, or as plain `@name` without one. */
export function MentionNode({ view: View, ...props }: Props) {
    if (View) return <View {...props} />;

    return <span className="mentions">@{props.node.attrs.name}</span>;
}
