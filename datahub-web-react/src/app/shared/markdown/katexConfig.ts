import rehypeKatex from 'rehype-katex';
import remarkMath from 'remark-math';

/**
 * Shared KaTeX configuration for markdown rendering
 * Enables LaTeX math support in markdown editors and viewers
 */
export const getKatexPlugins = () => ({
    rehypePlugins: [[rehypeKatex, { strict: false }]],
    remarkPlugins: [remarkMath],
});
