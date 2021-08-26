import React, { useMemo, useState } from 'react';
import { Button, Typography } from 'antd';

export type Props = {
    query?: string | null;
};

const UNEXPANDED_SIZE = 5;

export default function Query({ query }: Props) {
    const rows = useMemo(() => query?.split('\n'), [query]);

    const unexpandedRows = useMemo(() => rows?.slice(0, UNEXPANDED_SIZE), [rows]);

    const [expanded, setExpanded] = useState((rows?.length || 0) <= UNEXPANDED_SIZE);

    if (!query) {
        return null;
    }

    return (
        <Typography.Paragraph>
            {!expanded ? (
                <pre>
                    {unexpandedRows?.join('\n')}...
                    <Button type="link" onClick={() => setExpanded(true)}>
                        more
                    </Button>
                </pre>
            ) : (
                <pre>{query}</pre>
            )}
        </Typography.Paragraph>
    );
}
