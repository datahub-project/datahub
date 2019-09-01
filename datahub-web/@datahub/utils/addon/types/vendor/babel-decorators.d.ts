export interface IDecoratorElementObject {
  key: string;
  placement: 'prototype' | 'own';
  kind: 'method' | 'property';
  descriptor: PropertyDescriptor;
}

export interface IClassDecorator {
  elements: Array<IDecoratorElementObject>;
}

// TODO: META-8262 TS needs to implement stage 2 decorators: https://github.com/Microsoft/TypeScript/wiki/Roadmap#future
// State 2 decorators type for properties/methods:
// (elementObject: IDecoratorElementObject) => IDecoratorElementObject;
// eslint-disable-next-line
export type Stage2MethodDecorator = (elementObject: any) => any;

// TODO: META-8262 TS needs to implement stage 2 decorators: https://github.com/Microsoft/TypeScript/wiki/Roadmap#future
// State 2 decorators type for class:
// (classDecoratorArg: IClassDecorator) => IClassDecorator;
// eslint-disable-next-line
export type Stage2ClassDecorator = (classDecoratorArg: any) => any;
