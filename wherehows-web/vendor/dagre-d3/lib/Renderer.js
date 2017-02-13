var layout = require('dagre').layout;

var d3;
try { d3 = require('d3'); } catch (_) { d3 = window.d3; }

module.exports = Renderer;

function Renderer() {
  // Set up defaults...
  this._layout = layout();

  this.drawNodes(defaultDrawNodes);
  this.drawEdgeLabels(defaultDrawEdgeLabels);
  this.drawEdgePaths(defaultDrawEdgePaths);
  this.positionNodes(defaultPositionNodes);
  this.positionEdgeLabels(defaultPositionEdgeLabels);
  this.positionEdgePaths(defaultPositionEdgePaths);
  this.zoomSetup(defaultZoomSetup);
  this.zoom(defaultZoom);
  this.transition(defaultTransition);
  this.postLayout(defaultPostLayout);
  this.postRender(defaultPostRender);

  this.edgeInterpolate('bundle');
  this.edgeTension(0.95);
}

Renderer.prototype.layout = function(layout) {
  if (!arguments.length) { return this._layout; }
  this._layout = layout;
  return this;
};

Renderer.prototype.drawNodes = function(drawNodes) {
  if (!arguments.length) { return this._drawNodes; }
  this._drawNodes = bind(drawNodes, this);
  return this;
};

Renderer.prototype.drawEdgeLabels = function(drawEdgeLabels) {
  if (!arguments.length) { return this._drawEdgeLabels; }
  this._drawEdgeLabels = bind(drawEdgeLabels, this);
  return this;
};

Renderer.prototype.drawEdgePaths = function(drawEdgePaths) {
  if (!arguments.length) { return this._drawEdgePaths; }
  this._drawEdgePaths = bind(drawEdgePaths, this);
  return this;
};

Renderer.prototype.positionNodes = function(positionNodes) {
  if (!arguments.length) { return this._positionNodes; }
  this._positionNodes = bind(positionNodes, this);
  return this;
};

Renderer.prototype.positionEdgeLabels = function(positionEdgeLabels) {
  if (!arguments.length) { return this._positionEdgeLabels; }
  this._positionEdgeLabels = bind(positionEdgeLabels, this);
  return this;
};

Renderer.prototype.positionEdgePaths = function(positionEdgePaths) {
  if (!arguments.length) { return this._positionEdgePaths; }
  this._positionEdgePaths = bind(positionEdgePaths, this);
  return this;
};

Renderer.prototype.transition = function(transition) {
  if (!arguments.length) { return this._transition; }
  this._transition = bind(transition, this);
  return this;
};

Renderer.prototype.zoomSetup = function(zoomSetup) {
  if (!arguments.length) { return this._zoomSetup; }
  this._zoomSetup = bind(zoomSetup, this);
  return this;
};

Renderer.prototype.zoom = function(zoom) {
  if (!arguments.length) { return this._zoom; }
  if (zoom) {
    this._zoom = bind(zoom, this);
  } else {
    delete this._zoom;
  }
  return this;
};

Renderer.prototype.postLayout = function(postLayout) {
  if (!arguments.length) { return this._postLayout; }
  this._postLayout = bind(postLayout, this);
  return this;
};

Renderer.prototype.postRender = function(postRender) {
  if (!arguments.length) { return this._postRender; }
  this._postRender = bind(postRender, this);
  return this;
};

Renderer.prototype.edgeInterpolate = function(edgeInterpolate) {
  if (!arguments.length) { return this._edgeInterpolate; }
  this._edgeInterpolate = edgeInterpolate;
  return this;
};

Renderer.prototype.edgeTension = function(edgeTension) {
  if (!arguments.length) { return this._edgeTension; }
  this._edgeTension = edgeTension;
  return this;
};

Renderer.prototype.run = function(graph, orgSvg) {
  // First copy the input graph so that it is not changed by the rendering
  // process.
  graph = copyAndInitGraph(graph);

  // Create zoom elements
  var svg = this._zoomSetup(graph, orgSvg);

  // Create layers
  svg
    .selectAll('g.edgePaths, g.edgeLabels, g.nodes')
    .data(['edgePaths', 'edgeLabels', 'nodes'])
    .enter()
      .append('g')
      .attr('class', function(d) { return d; });

  // Create node and edge roots, attach labels, and capture dimension
  // information for use with layout.
  var svgNodes = this._drawNodes(graph, svg.select('g.nodes'));
  var svgEdgeLabels = this._drawEdgeLabels(graph, svg.select('g.edgeLabels'));

  svgNodes.each(function(u) { calculateDimensions(this, graph.node(u)); });
  svgEdgeLabels.each(function(e) { calculateDimensions(this, graph.edge(e)); });

  // Now apply the layout function
  var result = runLayout(graph, this._layout);

  // Copy useDef attribute from input graph to output graph
  graph.eachNode(function(u, a) {
    if (a.useDef) {
      result.node(u).useDef = a.useDef;
    }
  });

  // Run any user-specified post layout processing
  this._postLayout(result, svg);

  var svgEdgePaths = this._drawEdgePaths(graph, svg.select('g.edgePaths'));

  // Apply the layout information to the graph
  this._positionNodes(result, svgNodes);
  this._positionEdgeLabels(result, svgEdgeLabels);
  this._positionEdgePaths(result, svgEdgePaths, orgSvg);

  this._postRender(result, svg);

  return result;
};

function copyAndInitGraph(graph) {
  var copy = graph.copy();

  if (copy.graph() === undefined) {
    copy.graph({});
  }

  if (!('arrowheadFix' in copy.graph())) {
    copy.graph().arrowheadFix = true;
  }

  // Init labels if they were not present in the source graph
  copy.nodes().forEach(function(u) {
    var value = copyObject(copy.node(u));
    copy.node(u, value);
    if (!('label' in value)) { value.label = ''; }
  });

  copy.edges().forEach(function(e) {
    var value = copyObject(copy.edge(e));
    copy.edge(e, value);
    if (!('label' in value)) { value.label = ''; }
  });

  return copy;
}

function copyObject(obj) {
  var copy = {};
  for (var k in obj) {
    copy[k] = obj[k];
  }
  return copy;
}

function calculateDimensions(group, value) {
  var bbox = group.getBBox();
  value.width = bbox.width;
  value.height = bbox.height;
}

function runLayout(graph, layout) {
  var result = layout.run(graph);

  // Copy labels to the result graph
  graph.eachNode(function(u, value) { result.node(u).label = value.label; });
  graph.eachEdge(function(e, u, v, value) { result.edge(e).label = value.label; });

  return result;
}

function defaultDrawNodes(g, root) {
  var nodes = g.nodes().filter(function(u) { return !isComposite(g, u); });

  var svgNodes = root
    .selectAll('g.node')
    .classed('enter', false)
    .data(nodes, function(u) { return u; });

  svgNodes.selectAll('*').remove();

  svgNodes
    .enter()
      .append('g')
        .style('opacity', 0)
        .attr('class', 'node enter');

  svgNodes.each(function(u) {
    var attrs = g.node(u),
        domNode = d3.select(this);
    addLabel(attrs, domNode, true, 10, 10);
  });

  this._transition(svgNodes.exit())
      .style('opacity', 0)
      .remove();

  return svgNodes;
}

function defaultDrawEdgeLabels(g, root) {
  var svgEdgeLabels = root
    .selectAll('g.edgeLabel')
    .classed('enter', false)
    .data(g.edges(), function (e) { return e; });

  svgEdgeLabels.selectAll('*').remove();

  svgEdgeLabels
    .enter()
      .append('g')
        .style('opacity', 0)
        .attr('class', 'edgeLabel enter');

  svgEdgeLabels.each(function(e) { addLabel(g.edge(e), d3.select(this), false, 0, 0); });

  this._transition(svgEdgeLabels.exit())
      .style('opacity', 0)
      .remove();

  return svgEdgeLabels;
}

var defaultDrawEdgePaths = function(g, root) {
  var svgEdgePaths = root
    .selectAll('g.edgePath')
    .classed('enter', false)
    .data(g.edges(), function(e) { return e; });

  var DEFAULT_ARROWHEAD = 'url(#arrowhead)',
      createArrowhead = DEFAULT_ARROWHEAD;
  if (!g.isDirected()) {
    createArrowhead = null;
  } else if (g.graph().arrowheadFix !== 'false' && g.graph().arrowheadFix !== false) {
    createArrowhead = function() {
      var strokeColor = d3.select(this).style('stroke');
      if (strokeColor) {
        var id = 'arrowhead-' + strokeColor.replace(/[^a-zA-Z0-9]/g, '_');
        getOrMakeArrowhead(root, id).style('fill', strokeColor);
        return 'url(#' + id + ')';
      }
      return DEFAULT_ARROWHEAD;
    };
  }

  svgEdgePaths
    .enter()
      .append('g')
        .attr('class', 'edgePath enter')
        .append('path')
          .style('opacity', 0);

  svgEdgePaths
    .selectAll('path')
    .each(function(e) { applyStyle(g.edge(e).style, d3.select(this)); })
    .attr('marker-end', createArrowhead);

  this._transition(svgEdgePaths.exit())
      .style('opacity', 0)
      .remove();

  return svgEdgePaths;
};

function defaultPositionNodes(g, svgNodes) {
  function transform(u) {
    var value = g.node(u);
    return 'translate(' + value.x + ',' + value.y + ')';
  }

  // For entering nodes, position immediately without transition
  svgNodes.filter('.enter').attr('transform', transform);

  this._transition(svgNodes)
      .style('opacity', 1)
      .attr('transform', transform);
}

function defaultPositionEdgeLabels(g, svgEdgeLabels) {
  function transform(e) {
    var value = g.edge(e);
    var point = findMidPoint(value.points);
    return 'translate(' + point.x + ',' + point.y + ')';
  }

  // For entering edge labels, position immediately without transition
  svgEdgeLabels.filter('.enter').attr('transform', transform);

  this._transition(svgEdgeLabels)
    .style('opacity', 1)
    .attr('transform', transform);
}

function isEllipse(obj) {
  return Object.prototype.toString.call(obj) === '[object SVGEllipseElement]';
}

function isCircle(obj) {
  return Object.prototype.toString.call(obj) === '[object SVGCircleElement]';
}

function isPolygon(obj) {
  return Object.prototype.toString.call(obj) === '[object SVGPolygonElement]';
}

function intersectNode(nd, p1, root) {
  if (nd.useDef) {
    var definedFig = root.select('defs #' + nd.useDef).node();
    if (definedFig) {
      var outerFig = definedFig.childNodes[0];
      if (isCircle(outerFig) || isEllipse(outerFig)) {
        return intersectEllipse(nd, outerFig, p1);
      } else if (isPolygon(outerFig)) {
        return intersectPolygon(nd, outerFig, p1);
      }
    }
  }
  // TODO: use bpodgursky's shortening algorithm here
  return intersectRect(nd, p1);
}

function defaultPositionEdgePaths(g, svgEdgePaths, root) {
  var interpolate = this._edgeInterpolate,
      tension = this._edgeTension;

  function calcPoints(e) {
    var value = g.edge(e);
    var source = g.node(g.incidentNodes(e)[0]);
    var target = g.node(g.incidentNodes(e)[1]);
    var points = value.points.slice();

    var p0 = points.length === 0 ? target : points[0];
    var p1 = points.length === 0 ? source : points[points.length - 1];

    points.unshift(intersectNode(source, p0, root));
    points.push(intersectNode(target, p1, root));

    return d3.svg.line()
      .x(function(d) { return d.x; })
      .y(function(d) { return d.y; })
      .interpolate(interpolate)
      .tension(tension)
      (points);
  }

  svgEdgePaths.filter('.enter').selectAll('path')
      .attr('d', calcPoints);

  this._transition(svgEdgePaths.selectAll('path'))
      .attr('d', calcPoints)
      .style('opacity', 1);
}

// By default we do not use transitions
function defaultTransition(selection) {
  return selection;
}

// Setup dom for zooming
function defaultZoomSetup(graph, svg) {
  var root = svg.property('ownerSVGElement');
  // If the svg node is the root, we get null, so set to svg.
  if (!root) {
    root = svg;
  } else {
    root = d3.select(root);
  }

  if (root.select('rect.overlay').empty()) {
    // Create an overlay for capturing mouse events that don't touch foreground
    root.insert('rect', ':first-child')
      .attr('class', 'overlay')
      .attr('width', '100%')
      .attr('height', '100%')
      .style('fill', 'none')
      .style('pointer-events', 'all');

    // Capture the zoom behaviour from the svg
    svg = svg.append('g')
      .attr('class', 'zoom');

    if (this._zoom) {
      root.call(this._zoom(graph, svg));
    }
  }

  return svg;
}

// By default allow pan and zoom
function defaultZoom(graph, svg) {
  return d3.behavior.zoom().on('zoom', function() {
    svg.attr('transform', 'translate(' + d3.event.translate + ')scale(' + d3.event.scale + ')');
  });
}

function defaultPostLayout() {
  // Do nothing
}

function defaultPostRender(graph, root) {
  if (graph.isDirected()) {
    // Fill = #333 is for backwards compatibility
    getOrMakeArrowhead(root, 'arrowhead')
      .attr('fill', '#333');
  }
}

function getOrMakeArrowhead(root, id) {
  var search = root.select('#' + id);
  if (!search.empty()) { return search; }

  var defs = root.select('defs');
  if (defs.empty()) {
    defs = root.append('svg:defs');
  }

  var marker =
    defs
      .append('svg:marker')
        .attr('id', id)
        .attr('viewBox', '0 0 10 10')
        .attr('refX', 8)
        .attr('refY', 5)
        .attr('markerUnits', 'strokeWidth')
        .attr('markerWidth', 8)
        .attr('markerHeight', 5)
        .attr('orient', 'auto');

  marker
    .append('svg:path')
      .attr('d', 'M 0 0 L 10 5 L 0 10 z');

  return marker;
}

function addLabel(node, root, addingNode, marginX, marginY) {
  // If the node has 'useDef' meta data, we rely on that
  if (node.useDef) {
    root.append('use').attr('xlink:href', '#' + node.useDef);
    return;
  }
  // Add the rect first so that it appears behind the label
  var label = node.label;
  var rect = root.append('rect');
  if (node.width) {
    rect.attr('width', node.width);
  }
  if (node.height) {
    rect.attr('height', node.height);
  }

  var labelSvg = root.append('g'),
      innerLabelSvg;

  if (label[0] === '<') {
    addForeignObjectLabel(label, labelSvg);
    // No margin for HTML elements
    marginX = marginY = 0;
  } else {
    innerLabelSvg = addTextLabel(label,
                                 labelSvg,
                                 Math.floor(node.labelCols),
                                 node.labelCut);
    applyStyle(node.labelStyle, innerLabelSvg);
  }

  var labelBBox = labelSvg.node().getBBox();
  labelSvg.attr('transform',
                'translate(' + (-labelBBox.width / 2) + ',' + (-labelBBox.height / 2) + ')');

  var bbox = root.node().getBBox();

  rect
    .attr('rx', node.rx ? node.rx : 5)
    .attr('ry', node.ry ? node.ry : 5)
    .attr('x', -(bbox.width / 2 + marginX))
    .attr('y', -(bbox.height / 2 + marginY))
    .attr('width', bbox.width + 2 * marginX)
    .attr('height', bbox.height + 2 * marginY)
    .attr('fill', '#fff');

  if (addingNode) {
    applyStyle(node.style, rect);

    if (node.fill) {
      rect.style('fill', node.fill);
    }

    if (node.stroke) {
      rect.style('stroke', node.stroke);
    }

    if (node['stroke-width']) {
      rect.style('stroke-width', node['stroke-width'] + 'px');
    }

    if (node['stroke-dasharray']) {
      rect.style('stroke-dasharray', node['stroke-dasharray']);
    }

    if (node.href) {
      root
        .attr('class', root.attr('class') + ' clickable')
        .on('click', function() {
          window.open(node.href);
        });
    }
  }
}

function addForeignObjectLabel(label, root) {
  var fo = root
    .append('foreignObject')
      .attr('width', '100000');

  var w, h;
  fo
    .append('xhtml:div')
      .style('float', 'left')
      // TODO find a better way to get dimensions for foreignObjects...
      .html(function() { return label; })
      .each(function() {
        w = this.clientWidth;
        h = this.clientHeight;
      });

  fo
    .attr('width', w)
    .attr('height', h);
}

function addTextLabel(label, root, labelCols, labelCut) {
  if (labelCut === undefined) { labelCut = 'false'; }
  labelCut = (labelCut.toString().toLowerCase() === 'true');

  var node = root
    .append('text')
    .attr('text-anchor', 'left');

  label = label.replace(/\\n/g, '\n');

  var arr = labelCols ? wordwrap(label, labelCols, labelCut) : label;
  arr = arr.split('\n');
  for (var i = 0; i < arr.length; i++) {
    node
      .append('tspan')
        .attr('dy', '1em')
        .attr('x', '1')
        .text(arr[i]);
  }

  return node;
}

// Thanks to
// http://james.padolsey.com/javascript/wordwrap-for-javascript/
function wordwrap (str, width, cut, brk) {
  brk = brk || '\n';
  width = width || 75;
  cut = cut || false;

  if (!str) { return str; }

  var regex = '.{1,' + width + '}(\\s|$)' + (cut ? '|.{' + width + '}|.+$' : '|\\S+?(\\s|$)');

  return str.match(new RegExp(regex, 'g')).join(brk);
}

function findMidPoint(points) {
  var midIdx = points.length / 2;
  if (points.length % 2) {
    return points[Math.floor(midIdx)];
  } else {
    var p0 = points[midIdx - 1];
    var p1 = points[midIdx];
    return {x: (p0.x + p1.x) / 2, y: (p0.y + p1.y) / 2};
  }
}

function intersectRect(rect, point) {
  var x = rect.x;
  var y = rect.y;

  // Rectangle intersection algorithm from:
  // http://math.stackexchange.com/questions/108113/find-edge-between-two-boxes
  var dx = point.x - x;
  var dy = point.y - y;
  var w = rect.width / 2;
  var h = rect.height / 2;

  var sx, sy;
  if (Math.abs(dy) * w > Math.abs(dx) * h) {
    // Intersection is top or bottom of rect.
    if (dy < 0) {
      h = -h;
    }
    sx = dy === 0 ? 0 : h * dx / dy;
    sy = h;
  } else {
    // Intersection is left or right of rect.
    if (dx < 0) {
      w = -w;
    }
    sx = w;
    sy = dx === 0 ? 0 : w * dy / dx;
  }

  return {x: x + sx, y: y + sy};
}

function intersectEllipse(node, ellipseOrCircle, point) {
  // Formulae from: http://mathworld.wolfram.com/Ellipse-LineIntersection.html

  var cx = node.x;
  var cy = node.y;
  var rx, ry;

  if (isCircle(ellipseOrCircle)) {
    rx = ry = ellipseOrCircle.r.baseVal.value;
  } else {
    rx = ellipseOrCircle.rx.baseVal.value;
    ry = ellipseOrCircle.ry.baseVal.value;
  }

  var px = cx - point.x;
  var py = cy - point.y;

  var det = Math.sqrt(rx * rx * py * py + ry * ry * px * px);

  var dx = Math.abs(rx * ry * px / det);
  if (point.x < cx) {
    dx = -dx;
  }
  var dy = Math.abs(rx * ry * py / det);
  if (point.y < cy) {
    dy = -dy;
  }

  return {x: cx + dx, y: cy + dy};
}

function sameSign(r1, r2) {
  return r1 * r2 > 0;
}

// Add point to the found intersections, but check first that it is unique.
function addPoint(x, y, intersections) {
  if (!intersections.some(function (elm) { return elm[0] === x && elm[1] === y; })) {
    intersections.push([x, y]);
  }
}

function intersectLine(x1, y1, x2, y2, x3, y3, x4, y4, intersections) {
  // Algorithm from J. Avro, (ed.) Graphics Gems, No 2, Morgan Kaufmann, 1994, p7 and p473.

  var a1, a2, b1, b2, c1, c2;
  var r1, r2 , r3, r4;
  var denom, offset, num;
  var x, y;

  // Compute a1, b1, c1, where line joining points 1 and 2 is F(x,y) = a1 x + b1 y + c1 = 0.
  a1 = y2 - y1;
  b1 = x1 - x2;
  c1 = (x2 * y1) - (x1 * y2);

  // Compute r3 and r4.
  r3 = ((a1 * x3) + (b1 * y3) + c1);
  r4 = ((a1 * x4) + (b1 * y4) + c1);

  // Check signs of r3 and r4. If both point 3 and point 4 lie on
  // same side of line 1, the line segments do not intersect.
  if ((r3 !== 0) && (r4 !== 0) && sameSign(r3, r4)) {
    return /*DONT_INTERSECT*/;
  }

  // Compute a2, b2, c2 where line joining points 3 and 4 is G(x,y) = a2 x + b2 y + c2 = 0
  a2 = y4 - y3;
  b2 = x3 - x4;
  c2 = (x4 * y3) - (x3 * y4);

  // Compute r1 and r2
  r1 = (a2 * x1) + (b2 * y1) + c2;
  r2 = (a2 * x2) + (b2 * y2) + c2;

  // Check signs of r1 and r2. If both point 1 and point 2 lie
  // on same side of second line segment, the line segments do
  // not intersect.
  if ((r1 !== 0) && (r2 !== 0) && (sameSign(r1, r2))) {
    return /*DONT_INTERSECT*/;
  }

  // Line segments intersect: compute intersection point.
  denom = (a1 * b2) - (a2 * b1);
  if (denom === 0) {
    return /*COLLINEAR*/;
  }

  offset = Math.abs(denom / 2);

  // The denom/2 is to get rounding instead of truncating. It
  // is added or subtracted to the numerator, depending upon the
  // sign of the numerator.
  num = (b1 * c2) - (b2 * c1);
  x = (num < 0) ? ((num - offset) / denom) : ((num + offset) / denom);

  num = (a2 * c1) - (a1 * c2);
  y = (num < 0) ? ((num - offset) / denom) : ((num + offset) / denom);

  // lines_intersect
  addPoint(x, y, intersections);
}

function intersectPolygon(node, polygon, point) {
  var x1 = node.x;
  var y1 = node.y;
  var x2 = point.x;
  var y2 = point.y;

  var intersections = [];
  var points = polygon.points;

  var minx = 100000, miny = 100000;
  for (var j = 0; j < points.numberOfItems; j++) {
    var p = points.getItem(j);
    minx = Math.min(minx, p.x);
    miny = Math.min(miny, p.y);
  }

  var left = x1 - node.width / 2 - minx;
  var top =  y1 - node.height / 2 - miny;

  for (var i = 0; i < points.numberOfItems; i++) {
    var p1 = points.getItem(i);
    var p2 = points.getItem(i < points.numberOfItems - 1 ? i + 1 : 0);
    intersectLine(x1, y1, x2, y2, left + p1.x, top + p1.y, left + p2.x, top + p2.y, intersections);
  }

  if (intersections.length === 1) {
    return {x: intersections[0][0], y: intersections[0][1]};
  }

  if (intersections.length > 1) {
    // More intersections, find the one nearest to edge end point
    intersections.sort(function(p, q) {
      var pdx = p[0] - point.x,
         pdy = p[1] - point.y,
         distp = Math.sqrt(pdx * pdx + pdy * pdy),

         qdx = q[0] - point.x,
         qdy = q[1] - point.y,
         distq = Math.sqrt(qdx * qdx + qdy * qdy);

      return (distp < distq) ? -1 : (distp === distq ? 0 : 1);
    });
    return {x: intersections[0][0], y: intersections[0][1]};
  } else {
    console.log('NO INTERSECTION FOUND, RETURN NODE CENTER', node);
    return node;
  }
}

function isComposite(g, u) {
  return 'children' in g && g.children(u).length;
}

function bind(func, thisArg) {
  // For some reason PhantomJS occassionally fails when using the builtin bind,
  // so we check if it is available and if not, use a degenerate polyfill.
  if (func.bind) {
    return func.bind(thisArg);
  }

  return function() {
    return func.apply(thisArg, arguments);
  };
}

function applyStyle(style, domNode) {
  if (style) {
    var currStyle = domNode.attr('style') || '';
    domNode.attr('style', currStyle + '; ' + style);
  }
}
