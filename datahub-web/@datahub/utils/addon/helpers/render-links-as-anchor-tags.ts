import { helper } from '@ember/component/helper';
import marked from 'marked';
// Will initialize Marked with gfm, tables, and renderer options
const markedRendererOverride = new marked.Renderer();

markedRendererOverride.link = (href: string, title: string, text: string): string =>
  `<a href='${href}' title='${title || text}' target='_blank'>${text}</a>`;

// Overrides marked's default renderer which wraps everything in a p tag
markedRendererOverride.paragraph = (text: string): string => text;

marked.setOptions({
  gfm: true,
  tables: true,
  renderer: markedRendererOverride
});

/**
 * Renders text in anchor tags if text is or contains a link
 * @param {string} text - field value to be rendered
 */
export function renderLinksAsAnchorTags([text]: [string]): string {
  return marked(text).htmlSafe();
}

export default helper(renderLinksAsAnchorTags);
