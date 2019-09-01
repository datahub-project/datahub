import { TestContext } from 'ember-test-helpers';

export const getText = (test: TestContext): string => {
  return (test.element.textContent || '').trim();
};

export const getTextNoSpaces = (test: TestContext): string => {
  return getText(test).replace(/\s/gi, '');
};

export const getTextNoSpacesFromElement = (element: Element): string => {
  return (element.textContent || '').replace(/\s/gi, '');
};

export const getTextNoSpacesFromElements = (elements: Array<Element>): string => {
  return elements.map(element => getTextNoSpacesFromElement(element)).join('');
};

export const getElement = (test: TestContext): Element => {
  const element = test.element;

  if (!element) {
    throw new Error(`Base element not found`);
  }

  return element;
};

export const querySelector = <E extends Element>(test: TestContext, selector: string): E => {
  const element = getElement(test);
  const selectedElement = element.querySelector<E>(selector);

  if (!selectedElement) {
    throw new Error(`Element ${selector} not found`);
  }

  return selectedElement;
};

export const querySelectorAll = <E extends Element>(test: TestContext, selector: string): NodeListOf<E> => {
  const element = getElement(test);
  const selectedElements = element.querySelectorAll<E>(selector);

  if (!selectedElements) {
    throw new Error(`Elements ${selector} not found`);
  }

  return selectedElements;
};
