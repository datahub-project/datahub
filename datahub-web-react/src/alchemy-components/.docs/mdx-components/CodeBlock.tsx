import { DocsContext, Source } from '@storybook/blocks';
import React from 'react';

export const CodeBlock = () => {
    const context = React.useContext(DocsContext);

    const { primaryStory } = (context ?? {}) as any;
    const component = primaryStory?.component;
    // Stories without a `component` (e.g. Toast's imperative API) or whose component has no docgen
    // info have nothing meaningful to import, so skip the snippet instead of crashing the page.
    const name = component?.__docgenInfo?.displayName ?? component?.displayName;

    if (!name) return null;

    return (
        <div>
            <Source
                code={`
					import { ${name} } from '@components';
				`}
                format
                dark
            />
        </div>
    );
};
