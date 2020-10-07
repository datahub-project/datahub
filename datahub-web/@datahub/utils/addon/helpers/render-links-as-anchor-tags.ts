import { helper } from '@ember/component/helper';
import marked from 'marked';
import DOMPurify from 'dompurify';
import { htmlSafe } from '@ember/string';

// Will initialize Marked with gfm, tables, and renderer options
const markedRendererOverride = new marked.Renderer();

markedRendererOverride.link = (href: string, title: string, text: string): string =>
  `<a href='${href}' title='${title || text}' target='_blank'>${text}</a>`;

marked.setOptions({
  gfm: true,
  tables: true,
  breaks: true,
  renderer: markedRendererOverride
});

/**
 * Helper method that does the following things.
 * 1. uses the `Marked` library to convert user input markdown text into HTML tags
 * 2. Uses the `DOMPurify` library to sanitize the converted text for XSS.
 * 3. Translates the above text back into template  friendly html
 *
 * Note: the htmlsafe() as part of `marked` is only semantics and doesn't truly deem it htmlsafe.
 * @param text text that needs it's markdown converted and content sanitized
 */
export function markdownAndSanitize(text: string): ReturnType<typeof htmlSafe> {
  // extracts user entered content, coverts it into markdown text
  const markdownText = marked(text).htmlSafe();
  // XSS Sanitizes the input text
  const sanitizedMarkdownText = DOMPurify.sanitize(markdownText);
  // ensures that the html in the text is not escaped by the templates
  return htmlSafe(sanitizedMarkdownText);
}

/**
 * Renders text in anchor tags if text is or contains a link
 * @param {string} text - field value to be rendered
 */
export function renderLinksAsAnchorTags([text]: [string]): ReturnType<typeof htmlSafe> {
  return markdownAndSanitize(text);
}

export default helper(renderLinksAsAnchorTags);
