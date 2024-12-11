import React from 'react';

import { Source, DocsContext } from '@storybook/blocks';

export const CodeBlock = () => {
    const context = React.useContext(DocsContext);

    const { primaryStory } = context as any;
    const component = context ? primaryStory.component.__docgenInfo.displayName : '';

    if (!context || !primaryStory) return null;

    return (
        <div>
            <Source
                code={`
					import { ${component} } from '@components';
				`}
                format
                dark
            />
        </div>
    );
};
