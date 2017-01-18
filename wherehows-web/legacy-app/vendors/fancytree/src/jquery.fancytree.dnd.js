/*!
 * jquery.fancytree.dnd.js
 *
 * Drag-and-drop support.
 * (Extension module for jquery.fancytree.js: https://github.com/mar10/fancytree/)
 *
 * Copyright (c) 2014, Martin Wendt (http://wwWendt.de)
 *
 * Released under the MIT license
 * https://github.com/mar10/fancytree/wiki/LicenseInfo
 *
 * @version @VERSION
 * @date @DATE
 */

;(function($, window, document, undefined) {

"use strict";

/* *****************************************************************************
 * Private functions and variables
 */
var logMsg = $.ui.fancytree.debug,
	didRegisterDnd = false;

/* Convert number to string and prepend +/-; return empty string for 0.*/
function offsetString(n){
	return n === 0 ? "" : (( n > 0 ) ? ("+" + n) : ("" + n));
}

/* *****************************************************************************
 * Drag and drop support
 */
function _initDragAndDrop(tree) {
	var dnd = tree.options.dnd || null;
	// Register 'connectToFancytree' option with ui.draggable
	if( dnd ) {
		_registerDnd();
	}
	// Attach ui.draggable to this Fancytree instance
	if(dnd && dnd.dragStart ) {
		tree.widget.element.draggable($.extend({
			addClasses: false,
			appendTo: "body",
			containment: false,
			delay: 0,
			distance: 4,
			revert: false,
			scroll: true, // to disable, also set css 'position: inherit' on ul.fancytree-container
			scrollSpeed: 7,
			scrollSensitivity: 10,
			// Delegate draggable.start, drag, and stop events to our handler
			connectToFancytree: true,
			// Let source tree create the helper element
			helper: function(event) {
				var sourceNode = $.ui.fancytree.getNode(event.target);
				if(!sourceNode){ // Dynatree issue 211
					// might happen, if dragging a table *header*
					return "<div>ERROR?: helper requested but sourceNode not found</div>";
				}
				return sourceNode.tree.ext.dnd._onDragEvent("helper", sourceNode, null, event, null, null);
			},
			start: function(event, ui) {
				var sourceNode = ui.helper.data("ftSourceNode");
				return !!sourceNode; // Abort dragging if no node could be found
			}
		}, tree.options.dnd.draggable));
	}
	// Attach ui.droppable to this Fancytree instance
	if(dnd && dnd.dragDrop) {
		tree.widget.element.droppable($.extend({
			addClasses: false,
			tolerance: "intersect",
			greedy: false
/*
			activate: function(event, ui) {
				logMsg("droppable - activate", event, ui, this);
			},
			create: function(event, ui) {
				logMsg("droppable - create", event, ui);
			},
			deactivate: function(event, ui) {
				logMsg("droppable - deactivate", event, ui);
			},
			drop: function(event, ui) {
				logMsg("droppable - drop", event, ui);
			},
			out: function(event, ui) {
				logMsg("droppable - out", event, ui);
			},
			over: function(event, ui) {
				logMsg("droppable - over", event, ui);
			}
*/
		}, tree.options.dnd.droppable));
	}
}

//--- Extend ui.draggable event handling --------------------------------------

function _registerDnd() {
	if(didRegisterDnd){
		return;
	}

	// Register proxy-functions for draggable.start/drag/stop

	$.ui.plugin.add("draggable", "connectToFancytree", {
		start: function(event, ui) {
			// 'draggable' was renamed to 'ui-draggable' since jQueryUI 1.10
			var draggable = $(this).data("ui-draggable") || $(this).data("draggable"),
				sourceNode = ui.helper.data("ftSourceNode") || null;

			if(sourceNode) {
				// Adjust helper offset, so cursor is slightly outside top/left corner
				draggable.offset.click.top = -2;
				draggable.offset.click.left = + 16;
				// Trigger dragStart event
				// TODO: when called as connectTo..., the return value is ignored(?)
				return sourceNode.tree.ext.dnd._onDragEvent("start", sourceNode, null, event, ui, draggable);
			}
		},
		drag: function(event, ui) {
			var isHelper,
				// 'draggable' was renamed to 'ui-draggable' since jQueryUI 1.10
				draggable = $(this).data("ui-draggable") || $(this).data("draggable"),
				sourceNode = ui.helper.data("ftSourceNode") || null,
				prevTargetNode = ui.helper.data("ftTargetNode") || null,
				targetNode = $.ui.fancytree.getNode(event.target);

			if(event.target && !targetNode){
				// We got a drag event, but the targetNode could not be found
				// at the event location. This may happen,
				// 1. if the mouse jumped over the drag helper,
				// 2. or if a non-fancytree element is dragged
				// We ignore it:
				isHelper = $(event.target).closest("div.fancytree-drag-helper,#fancytree-drop-marker").length > 0;
				if(isHelper){
					logMsg("Drag event over helper: ignored.");
					return;
				}
			}
			ui.helper.data("ftTargetNode", targetNode);
			// Leaving a tree node
			if(prevTargetNode && prevTargetNode !== targetNode ) {
				prevTargetNode.tree.ext.dnd._onDragEvent("leave", prevTargetNode, sourceNode, event, ui, draggable);
			}
			if(targetNode){
				if(!targetNode.tree.options.dnd.dragDrop) {
					// not enabled as drop target
				} else if(targetNode === prevTargetNode) {
					// Moving over same node
					targetNode.tree.ext.dnd._onDragEvent("over", targetNode, sourceNode, event, ui, draggable);
				}else{
					// Entering this node first time
					targetNode.tree.ext.dnd._onDragEvent("enter", targetNode, sourceNode, event, ui, draggable);
				}
			}
			// else go ahead with standard event handling
		},
		stop: function(event, ui) {
			// 'draggable' was renamed to 'ui-draggable' since jQueryUI 1.10
			var draggable = $(this).data("ui-draggable") || $(this).data("draggable"),
				sourceNode = ui.helper.data("ftSourceNode") || null,
				targetNode = ui.helper.data("ftTargetNode") || null,
//				mouseDownEvent = draggable._mouseDownEvent,
				eventType = event.type,
				dropped = (eventType === "mouseup" && event.which === 1);

			if(!dropped){
				logMsg("Drag was cancelled");
			}
			if(targetNode) {
				if(dropped){
					targetNode.tree.ext.dnd._onDragEvent("drop", targetNode, sourceNode, event, ui, draggable);
				}
				targetNode.tree.ext.dnd._onDragEvent("leave", targetNode, sourceNode, event, ui, draggable);
			}
			if(sourceNode){
				sourceNode.tree.ext.dnd._onDragEvent("stop", sourceNode, null, event, ui, draggable);
			}
		}
	});

	didRegisterDnd = true;
}


/* *****************************************************************************
 *
 */

$.ui.fancytree.registerExtension({
	name: "dnd",
	version: "0.1.0",
	// Default options for this extension.
	options: {
		// Make tree nodes draggable:
		dragStart: null,  // Callback(sourceNode, data), return true, to enable dnd
		dragStop: null,   // Callback(sourceNode, data)
//      helper: null,
		// Make tree nodes accept draggables
		autoExpandMS: 1000, // Expand nodes after n milliseconds of hovering.
		preventVoidMoves: true, // Prevent dropping nodes 'before self', etc.
		preventRecursiveMoves: true, // Prevent dropping nodes on own descendants
		focusOnClick: false, // Focus, although draggable cancels mousedown event (#270)
		dragEnter: null,  // Callback(targetNode, data)
		dragOver: null,   // Callback(targetNode, data)
		dragDrop: null,   // Callback(targetNode, data)
		dragLeave: null,  // Callback(targetNode, data)
		//
		draggable: null,  // Additional options passed to jQuery draggable
		droppable: null   // Additional options passed to jQuery droppable
	},

	treeInit: function(ctx){
		var tree = ctx.tree;
		this._super(ctx);
		// issue #270: draggable eats mousedown events
		if( tree.options.dnd.dragStart ){
			tree.$container.on("mousedown", function(event){
				if( !tree.hasFocus() && ctx.options.dnd.focusOnClick ) {
					var node = $.ui.fancytree.getNode(event);
					node.debug("Re-enable focus that was prevented by jQuery UI draggable.");
					// node.setFocus();
					// $(node.span).closest(":tabbable").focus();
					// $(event.target).trigger("focus");
					// $(event.target).closest(":tabbable").trigger("focus");
					$(event.target).closest(":tabbable").focus();
				}
			});
		}
		_initDragAndDrop(tree);
	},
	/* Override key handler in order to cancel dnd on escape.*/
	nodeKeydown: function(ctx) {
		var event = ctx.originalEvent;
		if( event.which === $.ui.keyCode.ESCAPE) {
			this._local._cancelDrag();
		}
		return this._super(ctx);
	},
	nodeClick: function(ctx) {
		// if( ctx.options.dnd.dragStart ){
		// 	ctx.tree.$container.focus();
		// }
		return this._super(ctx);
	},
	/* Display drop marker according to hitMode ('after', 'before', 'over', 'out', 'start', 'stop'). */
	_setDndStatus: function(sourceNode, targetNode, helper, hitMode, accept) {
		var posOpts,
			markerOffsetX = 0,
			markerAt = "center",
			instData = this._local,
			$source = sourceNode ? $(sourceNode.span) : null,
			$target = $(targetNode.span);

		if( !instData.$dropMarker ) {
			instData.$dropMarker = $("<div id='fancytree-drop-marker'></div>")
				.hide()
				.css({"z-index": 1000})
				.prependTo($(this.$div).parent());
//                .prependTo("body");
		}
//      this.$dropMarker.attr("class", hitMode);
		if(hitMode === "after" || hitMode === "before" || hitMode === "over"){
//          $source && $source.addClass("fancytree-drag-source");

//          $target.addClass("fancytree-drop-target");

			switch(hitMode){
			case "before":
				instData
				.$dropMarker.removeClass("fancytree-drop-after fancytree-drop-over")
					.addClass("fancytree-drop-before");
				markerAt = "top";
				break;
			case "after":
				instData.$dropMarker.removeClass("fancytree-drop-before fancytree-drop-over")
					.addClass("fancytree-drop-after");
				markerAt = "bottom";
				break;
			default:
				instData.$dropMarker.removeClass("fancytree-drop-after fancytree-drop-before")
					.addClass("fancytree-drop-over");
				$target.addClass("fancytree-drop-target");
				markerOffsetX = 8;
			}

			if( $.ui.fancytree.jquerySupports.positionMyOfs ){
				posOpts = {
					my: "left" + offsetString(markerOffsetX) + " center",
					at: "left " + markerAt,
					of: $target
				};
			} else {
				posOpts = {
					my: "left center",
					at: "left " + markerAt,
					of: $target,
					offset: "" + markerOffsetX + " 0"
				};
			}
			instData.$dropMarker
				.show()
				.position(posOpts);
//          helper.addClass("fancytree-drop-hover");
		} else {
//          $source && $source.removeClass("fancytree-drag-source");
			$target.removeClass("fancytree-drop-target");
			instData.$dropMarker.hide();
//          helper.removeClass("fancytree-drop-hover");
		}
		if(hitMode === "after"){
			$target.addClass("fancytree-drop-after");
		} else {
			$target.removeClass("fancytree-drop-after");
		}
		if(hitMode === "before"){
			$target.addClass("fancytree-drop-before");
		} else {
			$target.removeClass("fancytree-drop-before");
		}
		if(accept === true){
			if($source){
				$source.addClass("fancytree-drop-accept");
			}
			$target.addClass("fancytree-drop-accept");
			helper.addClass("fancytree-drop-accept");
		}else{
			if($source){
				$source.removeClass("fancytree-drop-accept");
			}
			$target.removeClass("fancytree-drop-accept");
			helper.removeClass("fancytree-drop-accept");
		}
		if(accept === false){
			if($source){
				$source.addClass("fancytree-drop-reject");
			}
			$target.addClass("fancytree-drop-reject");
			helper.addClass("fancytree-drop-reject");
		}else{
			if($source){
				$source.removeClass("fancytree-drop-reject");
			}
			$target.removeClass("fancytree-drop-reject");
			helper.removeClass("fancytree-drop-reject");
		}
	},

	/*
	 * Handles drag'n'drop functionality.
	 *
	 * A standard jQuery drag-and-drop process may generate these calls:
	 *
	 * draggable helper():
	 *     _onDragEvent("helper", sourceNode, null, event, null, null);
	 * start:
	 *     _onDragEvent("start", sourceNode, null, event, ui, draggable);
	 * drag:
	 *     _onDragEvent("leave", prevTargetNode, sourceNode, event, ui, draggable);
	 *     _onDragEvent("over", targetNode, sourceNode, event, ui, draggable);
	 *     _onDragEvent("enter", targetNode, sourceNode, event, ui, draggable);
	 * stop:
	 *     _onDragEvent("drop", targetNode, sourceNode, event, ui, draggable);
	 *     _onDragEvent("leave", targetNode, sourceNode, event, ui, draggable);
	 *     _onDragEvent("stop", sourceNode, null, event, ui, draggable);
	 */
	_onDragEvent: function(eventName, node, otherNode, event, ui, draggable) {
		if(eventName !== "over"){
			logMsg("tree.ext.dnd._onDragEvent(%s, %o, %o) - %o", eventName, node, otherNode, this);
		}
		var $helper, nodeOfs, relPos, relPos2,
			enterResponse, hitMode, r,
			opts = this.options,
			dnd = opts.dnd,
			ctx = this._makeHookContext(node, event, {otherNode: otherNode, ui: ui, draggable: draggable}),
			res = null,
			$nodeTag = $(node.span);

		switch (eventName) {
		case "helper":
			// Only event and node argument is available
			$helper = $("<div class='fancytree-drag-helper'><span class='fancytree-drag-helper-img' /></div>")
				.css({zIndex: 3, position: "relative"}) // so it appears above ext-wide selection bar
				.append($nodeTag.find("span.fancytree-title").clone());
			// DT issue 244: helper should be child of scrollParent
			$("ul.fancytree-container", node.tree.$div).append($helper);
			// Attach node reference to helper object
			$helper.data("ftSourceNode", node);
			// logMsg("helper=%o", $helper);
			// logMsg("helper.sourceNode=%o", $helper.data("ftSourceNode"));
			res = $helper;
			break;

		case "start":
			if( node.isStatusNode() ) {
				res = false;
			} else if(dnd.dragStart) {
				res = dnd.dragStart(node, ctx);
			}
			if(res === false) {
				this.debug("tree.dragStart() cancelled");
				//draggable._clear();
				// NOTE: the return value seems to be ignored (drag is not canceled, when false is returned)
				// TODO: call this._cancelDrag()?
				ui.helper.trigger("mouseup")
					.hide();
			} else {
				$nodeTag.addClass("fancytree-drag-source");
			}
			break;

		case "enter":
			if(dnd.preventRecursiveMoves && node.isDescendantOf(otherNode)){
				r = false;
			}else{
				r = dnd.dragEnter ? dnd.dragEnter(node, ctx) : null;
			}
			if(!r){
				// convert null, undefined, false to false
				res = false;
			}else if ( $.isArray(r) ) {
				// TODO: also accept passing an object of this format directly
				res = {
					over: ($.inArray("over", r) >= 0),
					before: ($.inArray("before", r) >= 0),
					after: ($.inArray("after", r) >= 0)
				};
			}else{
				res = {
					over: ((r === true) || (r === "over")),
					before: ((r === true) || (r === "before")),
					after: ((r === true) || (r === "after"))
				};
			}
			ui.helper.data("enterResponse", res);
			logMsg("helper.enterResponse: %o", res);
			break;

		case "over":
			enterResponse = ui.helper.data("enterResponse");
			hitMode = null;
			if(enterResponse === false){
				// Don't call dragOver if onEnter returned false.
//                break;
			} else if(typeof enterResponse === "string") {
				// Use hitMode from onEnter if provided.
				hitMode = enterResponse;
			} else {
				// Calculate hitMode from relative cursor position.
				nodeOfs = $nodeTag.offset();
				relPos = { x: event.pageX - nodeOfs.left,
						   y: event.pageY - nodeOfs.top };
				relPos2 = { x: relPos.x / $nodeTag.width(),
							y: relPos.y / $nodeTag.height() };

				if( enterResponse.after && relPos2.y > 0.75 ){
					hitMode = "after";
				} else if(!enterResponse.over && enterResponse.after && relPos2.y > 0.5 ){
					hitMode = "after";
				} else if(enterResponse.before && relPos2.y <= 0.25) {
					hitMode = "before";
				} else if(!enterResponse.over && enterResponse.before && relPos2.y <= 0.5) {
					hitMode = "before";
				} else if(enterResponse.over) {
					hitMode = "over";
				}
				// Prevent no-ops like 'before source node'
				// TODO: these are no-ops when moving nodes, but not in copy mode
				if( dnd.preventVoidMoves ){
					if(node === otherNode){
						logMsg("    drop over source node prevented");
						hitMode = null;
					}else if(hitMode === "before" && otherNode && node === otherNode.getNextSibling()){
						logMsg("    drop after source node prevented");
						hitMode = null;
					}else if(hitMode === "after" && otherNode && node === otherNode.getPrevSibling()){
						logMsg("    drop before source node prevented");
						hitMode = null;
					}else if(hitMode === "over" && otherNode && otherNode.parent === node && otherNode.isLastSibling() ){
						logMsg("    drop last child over own parent prevented");
						hitMode = null;
					}
				}
//                logMsg("hitMode: %s - %s - %s", hitMode, (node.parent === otherNode), node.isLastSibling());
				ui.helper.data("hitMode", hitMode);
			}
			// Auto-expand node (only when 'over' the node, not 'before', or 'after')
			if(hitMode === "over" && dnd.autoExpandMS && node.hasChildren() !== false && !node.expanded) {
				node.scheduleAction("expand", dnd.autoExpandMS);
			}
			if(hitMode && dnd.dragOver){
				// TODO: http://code.google.com/p/dynatree/source/detail?r=625
				ctx.hitMode = hitMode;
				res = dnd.dragOver(node, ctx);
			}
			// DT issue 332
//			this._setDndStatus(otherNode, node, ui.helper, hitMode, res!==false);
			this._local._setDndStatus(otherNode, node, ui.helper, hitMode, res!==false && hitMode !== null);
			break;

		case "drop":
			hitMode = ui.helper.data("hitMode");
			if(hitMode && dnd.dragDrop){
				ctx.hitMode = hitMode;
				dnd.dragDrop(node, ctx);
			}
			break;

		case "leave":
			// Cancel pending expand request
			node.scheduleAction("cancel");
			ui.helper.data("enterResponse", null);
			ui.helper.data("hitMode", null);
			this._local._setDndStatus(otherNode, node, ui.helper, "out", undefined);
			if(dnd.dragLeave){
				dnd.dragLeave(node, ctx);
			}
			break;

		case "stop":
			$nodeTag.removeClass("fancytree-drag-source");
			if(dnd.dragStop){
				dnd.dragStop(node, ctx);
			}
			break;

		default:
			$.error("Unsupported drag event: " + eventName);
		}
		return res;
	},

	_cancelDrag: function() {
		 var dd = $.ui.ddmanager.current;
		 if(dd){
			 dd.cancel();
		 }
	}
});
}(jQuery, window, document));
