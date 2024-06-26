import React from 'react';

import { Source } from '@storybook/blocks';

import { DocsContext } from '@storybook/addon-docs/blocks';

export const CodeBlock = () => {
	const context = React.useContext(DocsContext);
	const component = context ? context.primaryStory.component.__docgenInfo.displayName : '';

	if (!context || !context.primaryStory.component) return null;

	return (
		<div>
			<Source
				code={`
					import { ${component} } from '@components';
				`}
				language="typescript"
				format
				dark
			/>
		</div>
	);
};