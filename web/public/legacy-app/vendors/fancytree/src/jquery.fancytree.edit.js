/*!
 * jquery.fancytree.edit.js
 *
 * Make node titles editable.
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


/*******************************************************************************
 * Private functions and variables
 */

var isMac = /Mac/.test(navigator.platform),
	escapeHtml = $.ui.fancytree.escapeHtml,
	unescapeHtml = $.ui.fancytree.unescapeHtml;
	// modifiers = {shift: "shiftKey", ctrl: "ctrlKey", alt: "altKey", meta: "metaKey"},
	// specialKeys = {
	// 	8: "backspace", 9: "tab", 10: "return", 13: "return", 16: "shift", 17: "ctrl", 18: "alt", 19: "pause",
	// 	20: "capslock", 27: "esc", 32: "space", 33: "pageup", 34: "pagedown", 35: "end", 36: "home",
	// 	37: "left", 38: "up", 39: "right", 40: "down", 45: "insert", 46: "del",
	// 	96: "0", 97: "1", 98: "2", 99: "3", 100: "4", 101: "5", 102: "6", 103: "7",
	// 	104: "8", 105: "9", 106: "*", 107: "+", 109: "-", 110: ".", 111 : "/",
	// 	112: "f1", 113: "f2", 114: "f3", 115: "f4", 116: "f5", 117: "f6", 118: "f7", 119: "f8",
	// 	120: "f9", 121: "f10", 122: "f11", 123: "f12", 144: "numlock", 145: "scroll", 186: ";", 191: "/",
	// 	220: "\\", 222: "'", 224: "meta"
	// },
	// shiftNums = {
	// 	"`": "~", "1": "!", "2": "@", "3": "#", "4": "$", "5": "%", "6": "^", "7": "&",
	// 	"8": "*", "9": "(", "0": ")", "-": "_", "=": "+", ";": ": ", "'": "\"", ",": "<",
	// 	".": ">",  "/": "?",  "\\": "|"
	// };


// $.ui.fancytree.isKeydownEvent = function(e, code){
// 	var i, part, partmap, partlist = code.split("+"), len = parts.length;
// 	var c = String.fromCharCode(e.which).toLowerCase();
// 	for( i = 0; i < len; i++ ) {
// 	}
// 	alert (parts.unshift());
// 	alert (parts.unshift());
// 	alert (parts.unshift());
// };


/**
 * [ext-edit] Start inline editing of current node title.
 *
 * @alias FancytreeNode#editStart
 * @requires Fancytree
 */
$.ui.fancytree._FancytreeNodeClass.prototype.editStart = function(){
	var $input,
		node = this,
		tree = this.tree,
		local = tree.ext.edit,
		prevTitle = node.title,
		instOpts = tree.options.edit,
		$title = $(".fancytree-title", node.span),
		eventData = {node: node, tree: tree, options: tree.options};

	if( instOpts.beforeEdit.call(node, {type: "beforeEdit"}, eventData) === false){
		return false;
	}
	// beforeEdit may want to modify the title before editing
	prevTitle = node.title;

	node.debug("editStart");
	// Disable standard Fancytree mouse- and key handling
	tree.widget._unbind();
	// #116: ext-dnd prevents the blur event, so we have to catch outer clicks
	$(document).on("mousedown.fancytree-edit", function(event){
		if( ! $(event.target).hasClass("fancytree-edit-input") ){
			node.editEnd(true, event);
		}
	});

	// Replace node with <input>
	$input = $("<input />", {
		"class": "fancytree-edit-input",
		value: unescapeHtml(prevTitle)
	});
	if ( instOpts.adjustWidthOfs != null ) {
		$input.width($title.width() + instOpts.adjustWidthOfs);
	}
	if ( instOpts.inputCss != null ) {
		$input.css(instOpts.inputCss);
	}
	eventData.input = $input;

	$title.html($input);

	$.ui.fancytree.assert(!local.currentNode, "recursive edit");
	local.currentNode = this;
	// Focus <input> and bind keyboard handler
	$input
		.focus()
		.change(function(event){
			$input.addClass("fancytree-edit-dirty");
		}).keydown(function(event){
			switch( event.which ) {
			case $.ui.keyCode.ESCAPE:
				node.editEnd(false, event);
				break;
			case $.ui.keyCode.ENTER:
				node.editEnd(true, event);
				return false; // so we don't start editmode on Mac
			}
			event.stopPropagation();
		}).blur(function(event){
			return node.editEnd(true, event);
		});

	instOpts.edit.call(node, {type: "edit"}, eventData);
};


/**
 * [ext-edit] Stop inline editing.
 * @param {Boolean} [applyChanges=false]
 * @alias FancytreeNode#editEnd
 * @requires jquery.fancytree.edit.js
 */
$.ui.fancytree._FancytreeNodeClass.prototype.editEnd = function(applyChanges, _event){
	var node = this,
		tree = this.tree,
		local = tree.ext.edit,
		instOpts = tree.options.edit,
		$title = $(".fancytree-title", node.span),
		$input = $title.find("input.fancytree-edit-input"),
		newVal = $input.val(),
		dirty = $input.hasClass("fancytree-edit-dirty"),
		doSave = (applyChanges || (dirty && applyChanges !== false)) && (newVal !== node.title),
		eventData = {
			node: node, tree: tree, options: tree.options, originalEvent: _event,
			dirty: dirty,
			save: doSave,
			input: $input,
			value: newVal
			};

	if( instOpts.beforeClose.call(node, {type: "beforeClose"}, eventData) === false){
		return false;
	}
	if( doSave && instOpts.save.call(node, {type: "save"}, eventData) === false){
		return false;
	}
	$input
		.removeClass("fancytree-edit-dirty")
		.unbind();
	// Unbind outer-click handler
	$(document).off(".fancytree-edit");

	if( doSave ) {
		node.setTitle( escapeHtml(newVal) );
	}else{
		node.renderTitle();
	}
	// Re-enable mouse and keyboard handling
	tree.widget._bind();
	local.currentNode = null;
	node.setFocus();
	// Set keyboard focus, even if setFocus() claims 'nothing to do'
	$(tree.$container).focus();
	eventData.input = null;
	instOpts.close.call(node, {type: "close"}, eventData);
	return true;
};


$.ui.fancytree._FancytreeNodeClass.prototype.startEdit = function(){
	this.warn("FancytreeNode.startEdit() is deprecated since 2014-01-04. Use .editStart() instead.");
	return this.editStart.apply(this, arguments);
};


$.ui.fancytree._FancytreeNodeClass.prototype.endEdit = function(){
	this.warn("FancytreeNode.endEdit() is deprecated since 2014-01-04. Use .editEnd() instead.");
	return this.editEnd.apply(this, arguments);
};


///**
// * Create a new child or sibling node.
// *
// * @param {String} [mode] 'before', 'after', or 'child'
// * @lends FancytreeNode.prototype
// * @requires jquery.fancytree.edit.js
// */
//$.ui.fancytree._FancytreeNodeClass.prototype.editCreateNode = function(mode){
//	var newNode,
//		node = this,
//		tree = this.tree,
//		local = tree.ext.edit,
//		instOpts = tree.options.edit,
//		$title = $(".fancytree-title", node.span),
//		$input = $title.find("input.fancytree-edit-input"),
//		newVal = $input.val(),
//		dirty = $input.hasClass("fancytree-edit-dirty"),
//		doSave = (applyChanges || (dirty && applyChanges !== false)) && (newVal !== node.title),
//		eventData = {
//			node: node, tree: tree, options: tree.options, originalEvent: _event,
//			dirty: dirty,
//			save: doSave,
//			input: $input,
//			value: newVal
//			};
//
//	node.debug("editCreate");
//
//	if( instOpts.beforeEdit.call(node, {type: "beforeCreateNode"}, eventData) === false){
//		return false;
//	}
//	newNode = this.addNode({title: "Neuer Knoten"}, mode);
//
//	newNode.editStart();
//};


/**
 * [ext-edit] Check if any node in this tree  in edit mode.
 *
 * @returns {FancytreeNode | null}
 * @alias Fancytree#isEditing
 * @requires jquery.fancytree.edit.js
 */
$.ui.fancytree._FancytreeClass.prototype.isEditing = function(){
	return this.ext.edit.currentNode;
};


/**
 * [ext-edit] Check if this node is in edit mode.
 * @returns {Boolean} true if node is currently beeing edited
 * @alias FancytreeNode#isEditing
 * @requires jquery.fancytree.edit.js
 */
$.ui.fancytree._FancytreeNodeClass.prototype.isEditing = function(){
	return this.tree.ext.edit.currentNode === this;
};


/*******************************************************************************
 * Extension code
 */
$.ui.fancytree.registerExtension({
	name: "edit",
	version: "0.1.0",
	// Default options for this extension.
	options: {
		adjustWidthOfs: 4,   // null: don't adjust input size to content
		inputCss: {minWidth: "3em"},
		triggerCancel: ["esc", "tab", "click"],
		// triggerStart: ["f2", "dblclick", "shift+click", "mac+enter"],
		triggerStart: ["f2", "shift+click", "mac+enter"],
		beforeClose: $.noop, // Return false to prevent cancel/save (data.input is available)
		beforeEdit: $.noop,  // Return false to prevent edit mode
		close: $.noop,       // Editor was removed
		edit: $.noop,        // Editor was opened (available as data.input)
//		keypress: $.noop,    // Not yet implemented
		save: $.noop         // Save data.input.val() or return false to keep editor open
	},
	// Local attributes
	currentNode: null,

	treeInit: function(ctx){
		this._super(ctx);
		this.$container.addClass("fancytree-ext-edit");
	},
	nodeClick: function(ctx) {
		if( $.inArray("shift+click", ctx.options.edit.triggerStart) >= 0 ){
			if( ctx.originalEvent.shiftKey ){
				ctx.node.editStart();
				return false;
			}
		}
		return this._super(ctx);
	},
	nodeDblclick: function(ctx) {
		if( $.inArray("dblclick", ctx.options.edit.triggerStart) >= 0 ){
			ctx.node.editStart();
			return false;
		}
		return this._super(ctx);
	},
	nodeKeydown: function(ctx) {
		switch( ctx.originalEvent.which ) {
		case 113: // [F2]
			if( $.inArray("f2", ctx.options.edit.triggerStart) >= 0 ){
				ctx.node.editStart();
				return false;
			}
			break;
		case $.ui.keyCode.ENTER:
			if( $.inArray("mac+enter", ctx.options.edit.triggerStart) >= 0 && isMac ){
				ctx.node.editStart();
				return false;
			}
			break;
		}
		return this._super(ctx);
	}
});
}(jQuery, window, document));
