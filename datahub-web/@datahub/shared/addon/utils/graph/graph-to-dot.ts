import { dasherize } from '@ember/string';
import { IGraphOptions } from '@datahub/shared/types/graph/graph-options';

type Node = Com.Linkedin.Metadata.Graph.Node;
type Graph = Com.Linkedin.Metadata.Graph.Graph;
type Edge = Com.Linkedin.Metadata.Graph.Edge;
type Attribute = Com.Linkedin.Metadata.Graph.Attribute;

/**
 * Bag of all attributes possible in GraphViz
 *
 * https://graphviz.gitlab.io/_pages/doc/info/shapes.html
 */
interface IAllAtributes {
  port?: string;
  colspan?: string;
  border?: string;
  align?: 'CENTER' | 'LEFT' | 'RIGHT' | 'TEXT';
  sides?: string; // LTBR (Left Top Bottom Right)
  bgcolor?: string;
  color?: string;
  cellboarder?: string;
  cellpadding?: string;
  cellspacing?: string;
  face?: string;
  pointSize?: string;
  // Graphviz won't render id without having href, post process required
  id?: string;
  href?: string;
}

/**
 * A graphViz pseudo html node
 *
 * https://graphviz.gitlab.io/_pages/doc/info/shapes.html
 */
interface IVizHtmlNode {
  type: string;
  children?: Array<IVizHtmlNode>;
  text?: string;
  attributes?: IAllAtributes;
}

/**
 * Options for font element
 */
type IFontOptions = Pick<IAllAtributes, 'color' | 'face' | 'pointSize'>;

/**
 * FontElement pseudo html node
 */
interface IFont extends IVizHtmlNode {
  type: 'FONT';
  children: Array<IVizHtmlNode>;
  attributes: IFontOptions;
}

/**
 * Options for cell element
 */
type ICellOptions = Pick<
  IAllAtributes,
  | 'port'
  | 'colspan'
  | 'border'
  | 'align'
  | 'sides'
  | 'bgcolor'
  | 'color'
  | 'cellpadding'
  | 'cellspacing'
  | 'id'
  | 'href'
>;

/**
 * CellElement pseudo html node
 */
interface ICell extends IVizHtmlNode {
  type: 'TD';
  children: Array<IVizHtmlNode>;
  attributes: ICellOptions;
}

/**
 * RowElement pseudo html node
 */
interface IRow extends IVizHtmlNode {
  type: 'TR';
  children: Array<IVizHtmlNode>;
}

/**
 * Options available for a table
 */
type ITableOptions = Pick<
  IAllAtributes,
  | 'port'
  | 'colspan'
  | 'border'
  | 'align'
  | 'sides'
  | 'bgcolor'
  | 'color'
  | 'cellboarder'
  | 'cellpadding'
  | 'cellspacing'
  | 'id'
  | 'href'
>;

/**
 * TableElement pseudo node
 */
interface ITable extends IVizHtmlNode {
  type: 'TABLE';
  children: Array<IVizHtmlNode>;
  attributes?: ITableOptions;
}

/**
 * Text special node that is a string
 */
interface IText extends IVizHtmlNode {
  type: 'text';
  text: string;
}

/**
 * Will transform a bag of options into attributes of a pseudo html element
 * for example: CELLSPACING="0" CELLPADDING="1"
 * @param options
 */
const domAttributesToString = (options: IAllAtributes): string =>
  Object.keys(options)
    .map((key: keyof IAllAtributes): string => {
      const value = options[key];
      const keyUppercased = dasherize(key).toUpperCase();
      return `${keyUppercased}="${value}"`;
    })
    .join(' ');

/**
 * Will render a node. It will also render the children.
 * @param nodes
 */
const render = (nodes: Array<IVizHtmlNode | undefined>): string =>
  (nodes.filter(Boolean) as Array<IVizHtmlNode>)
    .map(({ type, children, text, attributes }): string | undefined =>
      children ? `<${type} ${attributes ? domAttributesToString(attributes) : ''}>${render(children)}</${type}>` : text
    )
    .join('');

/**
 * Text element constructor
 * @param text
 */
const text = (text: string): IText => ({ type: 'text', text });

/**
 * Font element constructor
 * @param child
 * @param attributes
 */
const font = (child: IVizHtmlNode, attributes: IFontOptions): IFont => ({
  type: 'FONT',
  attributes,
  children: [child]
});

/**
 * Cell element constructor
 * @param child
 * @param attributes
 */
const cell = (child: IVizHtmlNode, attributes: ICellOptions): ICell => ({
  type: 'TD',
  attributes,
  children: [child]
});

/**
 * Row element constructor
 * @param children
 */
const row = (children: Array<ICell>): IRow => ({ type: 'TR', children });

/**
 * Table element constructor
 * @param children
 * @param attributes
 */
const table = (children: Array<IRow>, attributes?: ITableOptions): ITable => ({ type: 'TABLE', children, attributes });

/**
 * Will create a row for an attribute
 * @param attribute
 */
const attributeToRow = (attribute: Attribute & { value?: string }): IRow =>
  row([
    cell(
      table(
        [
          row([
            cell(text(attribute.name), {
              align: 'LEFT'
            }),
            cell(text(attribute.type || attribute.value || ''), {
              align: 'RIGHT'
            })
          ])
        ],
        {
          cellspacing: '0',
          border: '0',
          cellpadding: '10'
        }
      ),
      {
        port: attribute.name,
        id: `ENTITY-ATTRIBUTE::${attribute.name}${attribute.reference ? `::${attribute.reference}` : ''}`,
        href: '-',
        bgcolor: 'red'
      }
    )
  ]);

/**
 * Will create an array of rows for a list of attributes
 * @param attributes
 */
const attributesToRows = (attributes: Array<Attribute>, _options?: IGraphOptions): Array<IRow> =>
  attributes.map(attributeToRow);

/**
 * Will generate a toolbar for the node
 * @param node node to create the actions for
 * @param _options possible options needed
 */
const renderNodeToolbar = (node: Node, _options?: IGraphOptions): IVizHtmlNode =>
  table(
    [
      row([
        cell(text('View dataset detail <I>$1</I>'), {
          id: `ENTITY-ACTION-GO-TO-ENTITY::${node.id}`,
          href: '-',
          align: 'RIGHT'
        })
      ])
    ],
    {
      cellspacing: '0',
      border: '0',
      cellpadding: '0'
    }
  );

/**
 * Will render a node label using pseudo HTML elements
 * @param node
 */
const renderNodeLabel = (node: Node, options?: IGraphOptions): string =>
  `<${render([
    // Root table that will serve as a margin for arrow connection
    table(
      [
        row([
          cell(
            // Table that will contain title and attribtues
            table(
              [
                row([
                  // Title of the entity
                  cell(font(text(node.displayName || node.id), { pointSize: '20' }), {
                    align: 'LEFT',
                    id: `ENTITY-TITLE::${node.id}`,
                    href: '-',
                    cellpadding: '10',
                    cellspacing: '0',
                    bgcolor: 'red',
                    port: 'root'
                  })
                ]),
                // Append the attributes of the entity
                ...(node.attributes ? attributesToRows(node.attributes, options) : []),
                ...(node.entityUrn
                  ? [
                      row([
                        cell(renderNodeToolbar(node, options), {
                          align: 'LEFT',
                          cellpadding: '10',
                          cellspacing: '0',
                          bgcolor: 'red',
                          id: `ENTITY-ACTIONS::${node.id}`,
                          href: '-'
                        })
                      ])
                    ]
                  : [])
              ],
              {
                cellspacing: '0',
                border: '0',
                cellpadding: '0'
              }
            ),
            {}
          )
        ])
      ],
      { cellspacing: '10', border: '0' } // for arrow separation
    )
  ])}>`;

/**
 * Will return a dot notation node
 * @param node
 */
const renderNode = (node: Node, options?: IGraphOptions): string =>
  `"${node.id}" [
      id = "ENTITY::${node.id}"
      label = ${renderNodeLabel(node, options)}
    ]`;

/**
 * Will return a dot notation edges
 * @param node
 */
const renderEdges = (edges: Array<Edge>, _options?: IGraphOptions): Array<string> =>
  edges.map((edge): string => {
    const fromPort = edge.fromAttribute || 'root';
    const from = `"${edge.fromNode}":${fromPort}`;
    const to = `"${edge.toNode}":root`;
    const label = edge.attributes?.map(attr => `${attr.name}:${attr.value}`).join(' ');

    return `${from} -> ${to}  [
              id = "EDGE::${edge.fromNode}::${fromPort}::${edge.toNode}"
              ${label ? `label = "${label}"` : ''}
            ]`;
  });

/**
 * Will render all nodes
 * @param nodes
 */
const renderNodes = (nodes: Array<Node>, options?: IGraphOptions): Array<string> =>
  nodes.map(node => renderNode(node, options));

/**
 * Will render a graph into a dot notation
 *
 * https://graphviz.gitlab.io/_pages/doc/info/lang.html
 *
 * @param graph
 */
export function graphToDot(graph: Graph, options?: IGraphOptions): string {
  const dot = `
    digraph {
      graph [
        rankdir = "LR"
        bgcolor = "none"
      ];
      node [
        fontsize = "16"
        fontname = "helvetica, open-sans"
        shape = "none"
        margin = "0"
      ];
      edge [];
      ranksep = 2
      ${renderNodes(graph.nodes, options).join('\n')}
      ${graph.edges ? renderEdges(graph.edges, options).join('\n') : ''}
    }`;
  return dot;
}
