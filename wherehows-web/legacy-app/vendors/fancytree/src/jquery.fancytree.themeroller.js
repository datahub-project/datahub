/*!
 * jquery.fancytree.themeroller.js
 *
 * Enable jQuery UI ThemeRoller styles.
 * (Extension module for jquery.fancytree.js: https://github.com/mar10/fancytree/)
 *
 * @see http://jqueryui.com/themeroller/
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
 * Extension code
 */
$.ui.fancytree.registerExtension({
	name: "themeroller",
	version: "0.0.1",
	// Default options for this extension.
	options: {
		activeClass: "ui-state-active",
		foccusClass: "ui-state-focus",
		hoverClass: "ui-state-hover",
		selectedClass: "ui-state-highlight"
	},

	treeInit: function(ctx){
		this._super(ctx);
		var $el = ctx.widget.element;

		if($el[0].nodeName === "TABLE"){
			$el.addClass("ui-widget ui-corner-all");
			$el.find(">thead tr").addClass("ui-widget-header");
			$el.find(">tbody").addClass("ui-widget-conent");
		}else{
			$el.addClass("ui-widget ui-widget-content ui-corner-all");
		}

		$el.delegate(".fancytree-node", "mouseenter mouseleave", function(event){
			var node = $.ui.fancytree.getNode(event.target),
				flag = (event.type === "mouseenter");
			node.debug("hover: " + flag);
			$(node.span).toggleClass("ui-state-hover ui-corner-all", flag);
		});
	},
	treeDestroy: function(ctx){
		this._super(ctx);
		ctx.widget.element.removeClass("ui-widget ui-widget-content ui-corner-all");
	},
	nodeRenderStatus: function(ctx){
		var node = ctx.node,
			$el = $(node.span);
		this._super(ctx);
/*
		.ui-state-highlight: Class to be applied to highlighted or selected elements. Applies "highlight" container styles to an element and its child text, links, and icons.
		.ui-state-error: Class to be applied to error messaging container elements. Applies "error" container styles to an element and its child text, links, and icons.
		.ui-state-error-text: An additional class that applies just the error text color without background. Can be used on form labels for instance. Also applies error icon color to child icons.

		.ui-state-default: Class to be applied to clickable button-like elements. Applies "clickable default" container styles to an element and its child text, links, and icons.
		.ui-state-hover: Class to be applied on mouseover to clickable button-like elements. Applies "clickable hover" container styles to an element and its child text, links, and icons.
		.ui-state-focus: Class to be applied on keyboard focus to clickable button-like elements. Applies "clickable hover" container styles to an element and its child text, links, and icons.
		.ui-state-active: Class to be applied on mousedown to clickable button-like elements. Applies "clickable active" container styles to an element and its child text, links, and icons.
*/
		$el.toggleClass("ui-state-active", node.isActive());
		$el.toggleClass("ui-state-focus", node.hasFocus());
		$el.toggleClass("ui-state-highlight", node.isSelected());
//		node.debug("ext-themeroller.nodeRenderStatus: ", node.span.className);
	}
});
}(jQuery, window, document));
