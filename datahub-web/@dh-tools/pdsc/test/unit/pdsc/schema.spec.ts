// tslint:disable:object-literal-sort-keys object-literal-key-quotes
import chai = require('chai');

import * as pdsc from '../../../src/pdsc/schema';

const assert = chai.assert;

describe('pdsc', () => {
  const options: pdsc.SchemaOptions = {
    addType: undefined,
    ignorePackages: undefined
  };

  it('generates enum', () => {
    const raw = {
      type: 'enum',
      name: 'NonMemberPermissions',
      namespace: 'com.linkedin.restli.examples.groups.api',
      symbols: ['NONE', 'READ_ONLY']
    };
    const expected = `export type NonMemberPermissions = 'NONE'\n | 'READ_ONLY';\n`;
    const generated = pdsc.generateTs(raw as any, undefined, options);
    assert.equal(expected, generated.value);
  });

  it('generates typeref', () => {
    const raw = `{
          "type" : "typeref",
          "name" : "GroupMembershipQueryParamArrayRef",
          "namespace" : "com.linkedin.restli.examples.groups.api",
          "ref" : { "type" : "array", "items" : "GroupMembershipQueryParam" }
        }`;
    const expected =
      'export type GroupMembershipQueryParamArrayRef = Com.Linkedin.Restli.Examples.Groups.Api.GroupMembershipQueryParam[];\n';
    const generated = pdsc.generateTs(JSON.parse(raw) as any, undefined, options);
    assert.equal(expected, generated.value);
  });

  it('generates union typeref', () => {
    const raw = `
    {
      "type" : "typeref",
      "name" : "NotificationRecipient",
      "namespace" : "com.linkedin.dataConstructChangeManagement",
      "doc" : "A recipients of a change management notification",
      "ref" : [ {
        "alias" : "groupUrn",
        "type" : "com.linkedin.common.CorpGroupUrn"
      }, {
        "alias" : "userUrn",
        "type" : "com.linkedin.common.CorpuserUrn"
      } ]
    }`;
    const expected = 'export type NotificationRecipient = \n | {\ngroupUrn?: string,\nuserUrn?: string,\n}\n;\n';
    const generated = pdsc.generateTs(JSON.parse(raw) as any, undefined, options);
    assert.equal(expected, generated.value);
  });

  it('generates union typeref without alias', () => {
    const raw = `
    {
      "type" : "typeref",
      "name" : "NotificationRecipient",
      "namespace" : "com.linkedin.dataConstructChangeManagement",
      "doc" : "A recipients of a change management notification",
      "ref" : [ {
        "type" : "com.linkedin.common.CorpGroupUrn"
      }, {
        "type" : "com.linkedin.common.CorpuserUrn"
      } ]
    }`;
    const expected =
      "export type NotificationRecipient = \n | {\n'com.linkedin.common.CorpGroupUrn'?: string,\n'com.linkedin.common.CorpuserUrn'?: string,\n}\n;\n";
    const generated = pdsc.generateTs(JSON.parse(raw) as any, undefined, options);
    assert.equal(expected, generated.value);
  });

  it('generates union typeref without alias 2', () => {
    const raw = `
    {
      "type" : "typeref",
      "name" : "NotificationRecipient",
      "namespace" : "com.linkedin.namespace",
      "doc" : "A recipients of a change management notification",
      "ref" : [ {
        "type" : "CorpGroupUrn"
      }, {
        "type" : "CorpuserUrn"
      } ]
    }`;
    const expected =
      "export type NotificationRecipient = \n | {\n'com.linkedin.namespace.CorpGroupUrn'?: string,\n'com.linkedin.namespace.CorpuserUrn'?: string,\n}\n;\n";
    const generated = pdsc.generateTs(JSON.parse(raw) as any, undefined, options);
    assert.equal(expected, generated.value);
  });

  it('generates union typeref without alias 3', () => {
    const raw = `
    {
      "type" : "typeref",
      "name" : "FrameSourceProperties",
      "namespace" : "com.linkedin.feature.frame",
      "doc" : "A union of all supported source property types",
      "ref" : [ "ClassA", "ClassB" ]
    }`;
    const expected =
      "export type FrameSourceProperties = \n | {\n'com.linkedin.feature.frame.ClassA'?: Com.Linkedin.Feature.Frame.ClassA,\n'com.linkedin.feature.frame.ClassB'?: Com.Linkedin.Feature.Frame.ClassB,\n}\n;\n";
    const generated = pdsc.generateTs(JSON.parse(raw) as any, undefined, options);
    assert.equal(expected, generated.value);
  });

  it('generates union typeref without alias 4', () => {
    const raw = `
    {
      "type" : "typeref",
      "name" : "FrameSourceProperties",
      "namespace" : "com.linkedin.feature.frame",
      "doc" : "A union of all supported source property types",
      "ref" : [ "ClassA", "ClassB", "boolean" ]
    }`;
    const expected =
      "export type FrameSourceProperties = boolean\n | {\n'com.linkedin.feature.frame.ClassA'?: Com.Linkedin.Feature.Frame.ClassA,\n'com.linkedin.feature.frame.ClassB'?: Com.Linkedin.Feature.Frame.ClassB,\n}\n;\n";
    const generated = pdsc.generateTs(JSON.parse(raw) as any, undefined, options);
    assert.equal(expected, generated.value);
  });

  it('generates union typeref without alias 5', () => {
    const raw = `
    {
      "type" : "typeref",
      "name" : "FrameSourceProperties",
      "namespace" : "com.linkedin.feature.frame",
      "doc" : "A union of all supported source property types",
      "ref" : [ "ClassA"]
    }`;
    const expected =
      "export type FrameSourceProperties = \n | {\n'com.linkedin.feature.frame.ClassA': Com.Linkedin.Feature.Frame.ClassA,\n}\n;\n";
    const generated = pdsc.generateTs(JSON.parse(raw) as any, undefined, options);
    assert.equal(expected, generated.value);
  });

  it('generates union typeref without alias 6', () => {
    const raw = `
    {
      "type" : "typeref",
      "name" : "FrameFeatureClassification",
      "namespace" : "com.linkedin.feature.frame",
      "doc" : "A union of all supported feature classification types",
      "ref" : [ {
        "alias" : "classificationString",
        "type" : "string"
      } ]
    }`;
    const expected = 'export type FrameFeatureClassification = \n | {\nclassificationString: string,\n}\n;\n';
    const generated = pdsc.generateTs(JSON.parse(raw) as any, undefined, options);
    assert.equal(expected, generated.value);
  });

  it('generates empty typeref', () => {
    const raw = `
    {
      "type": "typeref",
      "name": "TransformationFunction",
      "namespace": "com.linkedin.proml.mlFeatureAnchor",
      "doc": "Represents the tranformation logic to produce feature value from the source of FeatureAnchor.\\nTODO(PROML-6165): Fill in concrete types after Frame v2 scope is finalized.",
      "ref": []
    }`;
    const expected = 'export type TransformationFunction = never;\n';
    const generated = pdsc.generateTs(JSON.parse(raw) as any, undefined, options);
    assert.equal(expected, generated.value);
  });

  it('generates record', () => {
    const raw = `{
        "type": "record",
        "name": "Fortune",
        "namespace": "org.restli.example.fortune",
         "doc": "Generate a fortune cookie",
        "fields": [
            {
                "name": "fortune",
                "type": "string",
                "doc": "The Fortune cookie string"
            },
            {
                "name": "funny_level",
                "type": "double",
                "optional":true,
                "doc": "The level of funniness"
            },
            {
                "name": "style",
                "type": "CookieStyle"
            },
            {
              "name": "facets",
              "type": { "type": "map", "values": "string" }
            },
            {
              "name": "action",
              "type": [
                "dismiss",
                "accept"
              ]
            }
        ]
     }`;
    // tslint:disable:max-line-length
    const expected =
      'export interface Fortune {\n  fortune: string;\n  funny_level?: number;\n  style: CookieStyle;\n  facets: {[id: string]: string};\n  action: dismiss\n | accept;\n}\n';
    const generated = pdsc.generateTs(JSON.parse(raw) as any, undefined, options);
    assert.equal(expected, generated.value);
    assert.equal('org.restli.example.fortune', generated.namespace);
    assert.equal('Fortune', generated.name);
  });

  it('generates fixed', () => {
    const raw = `{
      "type": "fixed",
      "name": "MD5",
      "namespace": "com.linkedin.common",
      "doc": "128 bit MD5 digest",
      "size" : 16
    }`;

    const expected = 'export type MD5 = string;\n';
    const generated = pdsc.generateTs(JSON.parse(raw) as any, undefined, options);
    assert.equal(expected, generated.value);
    assert.equal('MD5', generated.name);
    assert.equal('com.linkedin.common', generated.namespace);
  });

  it('generates boolean', () => {
    const raw = `{
      "type": "boolean",
      "name": "Awesome",
      "namespace": "com.linkedin.common"
    }`;

    const expected = 'export type Awesome = boolean;\n';
    const generated = pdsc.generateTs(JSON.parse(raw) as any, undefined, options);
    assert.equal(expected, generated.value);
  });

  it('generates array of array', () => {
    const raw = `{
           "name" : "stringArrayArray",
           "type" : { "type" : "array", "items" : { "type" : "array", "items" : "string" } },
           "namespace" : "org.test"
        }`;
    const expected = 'export type stringArrayArray = Array<string[]>;\n';
    const generated = pdsc.generateTs(JSON.parse(raw) as any, undefined, options);
    assert.equal(expected, generated.value);
  });

  it('generates flattened', () => {
    const raw = `{
            "doc" : "Map of attachment.",
            "type" : "map",
            "values" : "Attachment"
        }`;
    const expected = 'export type Bar = {[id: string]: Attachment};\n';
    const generated = pdsc.generateTs(JSON.parse(raw) as any, 'org/foo/Bar', options);
    assert.equal(expected, generated.value);
    assert.equal('org.foo', generated.namespace);
    assert.equal('Bar', generated.name);
  });
  it('handles empty record', () => {
    const raw = `{
      "type": "record",
      "name": "ErrorResponse",
      "namespace": "com.linkedin.restli.common",
      "doc": "A generic ErrorResponse",
      "fields": [
        {
          "name": "errorDetails",
          "type":
          {
            "type": "record",
            "name": "ErrorDetails",
            "fields": []
          },
          "optional": true
        }
      ]
    }`;
    const expected = 'export interface ErrorResponse {\n  errorDetails?: {\n  \n};\n}\n';
    const generated = pdsc.generateTs(JSON.parse(raw) as any, 'org/foo/Bar', options);
    assert.equal(expected, generated.value);
  });

  it('generates map', () => {
    const raw = `{
      "type": "record",
      "name": "FacetedFeedMetadata",
      "namespace": "com.linkedin.feed",
      "doc": "Metadata accompanying a feed service response, including pagination token.",
      "fields": [
          {
              "name": "facets",
              "doc": "Facet counts keyed by FPR id (first level key) and facet name (second level).",
              "type": {
                  "type": "map",
                  "values": {
                      "type": "map",
                      "values": "int"
                  }
              }
          }
      ]
    }`;
    // tslint:disable:max-line-length
    const expected = 'export interface FacetedFeedMetadata {\n  facets: {[id: string]: {[id: string]: number}};\n}\n';
    const generated = pdsc.generateTs(JSON.parse(raw) as any, undefined, options);
    assert.equal(expected, generated.value);
  });

  it('uses includes as interface extensions', () => {
    const raw = `{
      "type": "record",
      "name": "BaseRecord",
      "namespace": "com.linkedin.feed",
      "doc": "Metadata accompanying a feed service response, including pagination token.",
      "include" : [ "com.linkedin.common.Mixin" ],
      "fields": [
      ]
    }`;
    // tslint:disable:max-line-length
    const expected = 'export interface BaseRecord extends Linkedin.Common.Mixin {\n  \n}\n';
    const restrictiveOptions: pdsc.SchemaOptions = {
      addType: undefined,
      ignorePackages: [/com/]
    };
    const generated = pdsc.generateTs(JSON.parse(raw) as any, undefined, restrictiveOptions);
    assert.equal(expected, generated.value);
  });
});
