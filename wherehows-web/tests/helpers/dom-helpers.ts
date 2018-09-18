import { TestContext } from 'ember-test-helpers';

export const getText = (test: TestContext) => {
  return (test.element.textContent || '').trim();
};

export const getTextNoSpaces = (test: TestContext) => {
  return getText(test).replace(/\s/gi, '');
};

export const querySelector = <E extends Element>(test: TestContext, selector: string): E | null => {
  const element = test.element;

  if (!element) {
    return null;
  }

  return element.querySelector<E>(selector);
};
