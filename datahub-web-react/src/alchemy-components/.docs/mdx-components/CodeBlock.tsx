/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { DocsContext, Source } from '@storybook/blocks';
import React from 'react';

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
