import type { CSSProperties, ComponentType, HTMLAttributes, ReactNode } from 'react';
import type { RefractorSyntax } from 'refractor';

export interface PrismLightProps {
    language?: string;
    style?: { [key: string]: CSSProperties };
    showLineNumbers?: boolean;
    lineNumberStyle?: CSSProperties;
    wrapLongLines?: boolean;
    wrapLines?: boolean;
    lineProps?: (lineNumber: number) => HTMLAttributes<HTMLElement>;
    customStyle?: CSSProperties;
    children?: ReactNode;
    className?: string;
}

/**
 * `registerLanguage` ignores `name` and registers `language.displayName`.
 */
export type PrismLightComponent = ComponentType<PrismLightProps> & {
    registerLanguage: (name: string, language: RefractorSyntax) => void;
};
