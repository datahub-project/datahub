/**
 * Remove inline styles and apply a class
 * @param cssClass css class to be applied
 * @param el element to remove inline styles
 */
const removeInlineStylesAndAddClass = (cssClass: string, el: Element | null): void => {
  if (el) {
    el.removeAttribute('fill');
    el.removeAttribute('stroke');
    el.removeAttribute('font-size');
    el.removeAttribute('font-family');
    el.classList.add(cssClass);
  }
};

/**
 * Will remove a node but will save the children by appending them
 * to the parent element
 * @param el element to be removed
 */
const removeNodeButPreserveChildren = (el: HTMLElement): void => {
  const parent = el.parentElement;
  if (parent) {
    Array.from(el.children).forEach((child): void => {
      parent.appendChild(child);
    });
    parent.removeChild(el);
  }
};

/**
 * Simple alias for querySelectorAll and forEach
 * @param root Element to apply query
 * @param selector query
 * @param fn fn to execute on each node
 */
export const forEachNode = (root: Element | undefined, selector: string, fn: (el: Element) => void): void => {
  if (root) {
    root.querySelectorAll(selector).forEach(fn);
  }
};

/**
 * Process entity node. Will add data-entity-id
 * @param node
 */
const processNode = (node: Element): void => {
  const { id } = node;
  const [, entityId] = id.split('::');

  node.setAttribute('data-entity-id', entityId);
};

/**
 * Will process title of an entity.
 * @param baseClass
 */
const processNodeTitle = (baseClass: string) => (nodeTitle: Element): void => {
  nodeTitle.classList.add(`${baseClass}__title`);

  removeInlineStylesAndAddClass(`${baseClass}__title-bg`, nodeTitle.querySelector('polygon'));
  removeInlineStylesAndAddClass(`${baseClass}__title-text`, nodeTitle.querySelector('text'));
};

/**
 * Will process an attribute of an entity
 * @param baseClass
 */
const processNodeAttribute = (baseClass: string) => (nodeAttribute: Element): void => {
  const { id } = nodeAttribute;
  const [, propertyName, reference] = id.split('::');
  const typeElement = nodeAttribute.querySelector('text:nth-child(3)');
  const nameElement = nodeAttribute.querySelector('text:nth-child(2)');
  const backgroundElement = nodeAttribute.querySelector('polygon');

  nodeAttribute.classList.add(`${baseClass}__property`);

  removeInlineStylesAndAddClass(`${baseClass}__property-bg`, backgroundElement);
  removeInlineStylesAndAddClass(`${baseClass}__property-name`, nameElement);
  removeInlineStylesAndAddClass(`${baseClass}__property-type`, typeElement);

  if (reference && typeElement) {
    typeElement.classList.add(`${baseClass}__property-type--reference`);
    typeElement.setAttribute('data-reference-link', reference);

    nodeAttribute.setAttribute('data-reference', reference);
    nodeAttribute.setAttribute('data-property-name', propertyName);
  }
};

/**
 * Will process an edge
 * @param baseClass
 */
const processEdges = (baseClass: string) => (edge: Element): void => {
  const { id } = edge;
  const [, from, property, to] = id.split('::');

  edge.setAttribute('data-from', from);
  edge.setAttribute('data-property', property);
  edge.setAttribute('data-to', to);

  removeInlineStylesAndAddClass(`${baseClass}__connector`, edge.querySelector('path'));
  removeInlineStylesAndAddClass(`${baseClass}__connector-arrow`, edge.querySelector('polygon'));
  removeInlineStylesAndAddClass(`${baseClass}__connector-text`, edge.querySelector('text'));
};

/**
 * Will process a node toolbar removing extra inline styles and applying a base class
 * @param baseClass
 */
const processNodeToolbar = (baseClass: string) => (toolbar: Element): void => {
  toolbar.classList.add(`${baseClass}__toolbar`);

  removeInlineStylesAndAddClass(`${baseClass}__toolbar-bg`, toolbar.querySelector('polygon'));
};

/**
 * Will process a node toolbar action removing extra inline styles and applying a base class
 * @param baseClass
 */
const processNodeToolbarAction = (baseClass: string) => (toolbarAction: Element): void => {
  toolbarAction.classList.add(`${baseClass}__action`);

  removeInlineStylesAndAddClass(`${baseClass}__action-bg`, toolbarAction.querySelector('polygon'));
  removeInlineStylesAndAddClass(`${baseClass}__action-text`, toolbarAction.querySelector('text'));
};

/**
 * Will process a go to action by adding a data attribute with the data needed to generate a link
 * @param baseClass
 */
const processNodeToolbarActionGoTo = (baseClass: string) => (button: Element): void => {
  processNodeToolbarAction(baseClass);

  button.classList.add(`${baseClass}__action-go-to-entity`);
  const { id } = button;
  const [, to] = id.split('::');

  button.setAttribute('data-entity-urn', to);
};

/**
 * Map of key used in graph to the dom reference of the icon.
 */
const iconMap: Record<string, string> = {
  $1: '#icon-external-link'
};

/**
 * Since GraphViz can't render custom icons, we will use the tag <I> that will generate
 * <text style="italic"> tag to replace is with our custom svg icon. See iconMap for icons available
 * @param node
 */
const processIcons = (node: Element): void => {
  Object.keys(iconMap).forEach(iconKey => {
    if (node.textContent === iconKey) {
      const iconSelector = iconMap[iconKey];
      const iconSize = 512;
      const targetSize = 12;
      const x = node.getAttribute('x');
      const y = node.getAttribute('y');
      const transforms = [
        `translate(${iconSize / -2},${iconSize / -2})`,
        `translate(${x}, ${y})`,
        `translate(${iconSize / 2}, ${iconSize / 2})`,
        `scale(${targetSize / iconSize})`,
        `translate(0, ${-1 * iconSize})`
      ];
      const useElement = document.createElementNS('http://www.w3.org/2000/svg', 'use');
      useElement.setAttribute('href', iconSelector);
      useElement.setAttribute('transform', transforms.join(','));
      node.parentElement?.replaceChild(useElement, node);
    }
  });
};

/**
 * Will 'fix' the svg coming from graphViz. It will make it more suitable for interaction and custom styling
 * @param svg
 * @param baseClass
 */
export function processGraphVizSvg(svg: SVGSVGElement, baseClass: string): SVGSVGElement {
  // Remove title nodes as they are not useful
  forEachNode(svg, 'title', (el): void => el.remove());

  // Remove a but preserve children
  forEachNode(svg, 'a', removeNodeButPreserveChildren);

  // Remove prefix 'a_' from ids as GraphViz adds those unexpectly
  forEachNode(svg, '[id^="a_"]', (el): void => el.setAttribute('id', el.id.replace('a_', '')));

  // Process nodes root element
  forEachNode(svg, '[id^="ENTITY::"]', processNode);

  // Process title of nodes
  forEachNode(svg, '[id^="ENTITY-TITLE::"]', processNodeTitle(baseClass));

  // Process attributes of nodes
  forEachNode(svg, '[id^="ENTITY-ATTRIBUTE::"]', processNodeAttribute(baseClass));

  // Process edges
  forEachNode(svg, '[id^="EDGE::"]', processEdges(baseClass));

  // Process node toolbar
  forEachNode(svg, '[id^="ENTITY-ACTIONS::"]', processNodeToolbar(baseClass));

  // Process action 'go to entity'
  forEachNode(svg, '[id^="ENTITY-ACTION-GO-TO-ENTITY::"]', processNodeToolbarActionGoTo(baseClass));

  // Generate icons
  forEachNode(svg, 'text[font-style="italic"]', processIcons);
  return svg;
}
