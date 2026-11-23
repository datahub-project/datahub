import useTypographyStyle from 'antd/es/typography/style';
import { useMemo } from 'react';

const TYPOGRAPHY_CLASS = 'ant-typography';

/**
 * Class names that give the editor surface antd's typography styling (paragraph spacing, headings,
 * lists, code). antd 5 scopes those styles to its theme hash class, so the bare ant-typography class
 * that was enough in antd 4 no longer matches; this registers the styles and returns the hash too.
 */
export default function useEditorTypographyClassNames(): string[] {
    const [, hashId, cssVarClass] = useTypographyStyle(TYPOGRAPHY_CLASS);
    return useMemo(() => [TYPOGRAPHY_CLASS, hashId, cssVarClass].filter(Boolean), [hashId, cssVarClass]);
}
