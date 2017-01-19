/*!
 * jquery.fancytree.persist.js
 *
 * Persist tree status in cookiesRemove or highlight tree nodes, based on a filter.
 * (Extension module for jquery.fancytree.js: https://github.com/mar10/fancytree/)
 *
 * @depends: jquery.cookie.js
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
function _assert(cond, msg){
	msg = msg || "";
	if(!cond){
		$.error("Assertion failed " + msg);
	}
}

var ACTIVE = "active",
	EXPANDED = "expanded",
	FOCUS = "focus",
	SELECTED = "selected";


/* Recursively load lazy nodes
 * @param {string} mode 'load', 'expand', false
 */
function _loadLazyNodes(tree, local, keyList, mode, dfd) {
	var i, key, l, node,
		foundOne = false,
		deferredList = [],
		missingKeyList = [];

	keyList = keyList || [];
	dfd = dfd || $.Deferred();

	for( i=0, l=keyList.length; i<l; i++ ) {
		key = keyList[i];
		node = tree.getNodeByKey(key);
		if( node ) {
			if( mode && node.isUndefined() ) {
				foundOne = true;
				tree.debug("_loadLazyNodes: " + node + " is lazy: loading...");
				if( mode === "expand" ) {
					deferredList.push(node.setExpanded());
				} else {
					deferredList.push(node.load());
				}
			} else {
				tree.debug("_loadLazyNodes: " + node + " already loaded.");
				node.setExpanded();
			}
		} else {
			missingKeyList.push(key);
			tree.debug("_loadLazyNodes: " + node + " was not yet found.");
		}
	}

	$.when.apply($, deferredList).always(function(){
		// All lazy-expands have finished
		if( foundOne && missingKeyList.length > 0 ) {
			// If we read new nodes from server, try to resolve yet-missing keys
			_loadLazyNodes(tree, local, missingKeyList, mode, dfd);
		} else {
			if( missingKeyList.length ) {
				tree.warn("_loadLazyNodes: could not load those keys: ", missingKeyList);
				for( i=0, l=missingKeyList.length; i<l; i++ ) {
					key = keyList[i];
					local._appendKey(EXPANDED, keyList[i], false);
				}
			}
			dfd.resolve();
		}
	});
	return dfd;
}


/**
 * [ext-persist] Remove persistence cookies of the given type(s).
 * Called like
 *     $("#tree").fancytree("getTree").clearCookies("active expanded focus selected");
 *
 * @alias Fancytree#clearCookies
 * @requires jquery.fancytree.persist.js
 */
$.ui.fancytree._FancytreeClass.prototype.clearCookies = function(types){
	var local = this.ext.persist,
		prefix = local.cookiePrefix;

	types = types || "active expanded focus selected";
	if(types.indexOf(ACTIVE) >= 0){
		local._data(prefix + ACTIVE, null);
	}
	if(types.indexOf(EXPANDED) >= 0){
		local._data(prefix + EXPANDED, null);
	}
	if(types.indexOf(FOCUS) >= 0){
		local._data(prefix + FOCUS, null);
	}
	if(types.indexOf(SELECTED) >= 0){
		local._data(prefix + SELECTED, null);
	}
};


/**
 * [ext-persist] Return persistence information from cookies
 *
 * Called like
 *     $("#tree").fancytree("getTree").getPersistData();
 *
 * @alias Fancytree#getPersistData
 * @requires jquery.fancytree.persist.js
 */
$.ui.fancytree._FancytreeClass.prototype.getPersistData = function(){
	var local = this.ext.persist,
		prefix = local.cookiePrefix,
		delim = local.cookieDelimiter,
		res = {};

	res[ACTIVE] = local._data(prefix + ACTIVE);
	res[EXPANDED] = (local._data(prefix + EXPANDED) || "").split(delim);
	res[SELECTED] = (local._data(prefix + SELECTED) || "").split(delim);
	res[FOCUS] = local._data(prefix + FOCUS);
	return res;
};


/* *****************************************************************************
 * Extension code
 */
$.ui.fancytree.registerExtension({
	name: "persist",
	version: "0.3.0",
	// Default options for this extension.
	options: {
		cookieDelimiter: "~",
		cookiePrefix: undefined, // 'fancytree-<treeId>-' by default
		cookie: {
			raw: false,
			expires: "",
			path: "",
			domain: "",
			secure: false
		},
		expandLazy: false,     // true: recursively expand and load lazy nodes
		overrideSource: false, // true: cookie takes precedence over `source` data attributes.
		store: "auto",         // 'cookie': force cookie, 'local': force localStore, 'session': force sessionStore
		types: "active expanded focus selected"
	},

	/* Generic read/write string data to cookie, sessionStorage or localStorage. */
	_data: function(key, value){
		var ls = this._local.localStorage; // null, sessionStorage, or localStorage

		if( value === undefined ) {
			return ls ? ls.getItem(key) : $.cookie(key);
		} else if ( value === null ) {
			if( ls ) {
				ls.removeItem(key);
			} else {
				$.removeCookie(key);
			}
		} else {
			if( ls ) {
				ls.setItem(key, value);
			} else {
				$.cookie(key, value, this.options.persist.cookie);
			}
		}
	},

	/* Append `key` to a cookie. */
	_appendKey: function(type, key, flag){
		key = "" + key; // #90
		var local = this._local,
			instOpts = this.options.persist,
			delim = instOpts.cookieDelimiter,
			cookieName = local.cookiePrefix + type,
			data = local._data(cookieName),
			keyList = data ? data.split(delim) : [],
			idx = $.inArray(key, keyList);
		// Remove, even if we add a key,  so the key is always the last entry
		if(idx >= 0){
			keyList.splice(idx, 1);
		}
		// Append key to cookie
		if(flag){
			keyList.push(key);
		}
		local._data(cookieName, keyList.join(delim));
	},

	treeInit: function(ctx){
		var tree = ctx.tree,
			opts = ctx.options,
			local = this._local,
			instOpts = this.options.persist;

		// For 'auto' or 'cookie' mode, the cookie plugin must be available
		_assert(instOpts.store === "localStore" || $.cookie, "Missing required plugin for 'persist' extension: jquery.cookie.js");

		local.cookiePrefix = instOpts.cookiePrefix || ("fancytree-" + tree._id + "-");
		local.storeActive = instOpts.types.indexOf(ACTIVE) >= 0;
		local.storeExpanded = instOpts.types.indexOf(EXPANDED) >= 0;
		local.storeSelected = instOpts.types.indexOf(SELECTED) >= 0;
		local.storeFocus = instOpts.types.indexOf(FOCUS) >= 0;
		if( instOpts.store === "cookie" || !window.localStorage ) {
			local.localStorage = null;
		} else {
			local.localStorage = (instOpts.store === "local") ? window.localStorage : window.sessionStorage;
		}

		// Bind init-handler to apply cookie state
		tree.$div.bind("fancytreeinit", function(event){
			var cookie, dfd, i, keyList, node,
				prevFocus = $.cookie(local.cookiePrefix + FOCUS); // record this before node.setActive() overrides it;

			tree.debug("COOKIE " + document.cookie);

			cookie = local._data(local.cookiePrefix + EXPANDED);
			keyList = cookie && cookie.split(instOpts.cookieDelimiter);

			if( local.storeExpanded ) {
				// Recursively load nested lazy nodes if expandLazy is 'expand' or 'load'
				// Also remove expand-cookies for unmatched nodes
				dfd = _loadLazyNodes(tree, local, keyList, instOpts.expandLazy ? "expand" : false , null);
			} else {
				// nothing to do
				dfd = new $.Deferred().resolve();
			}

			dfd.done(function(){
				if(local.storeSelected){
					cookie = local._data(local.cookiePrefix + SELECTED);
					if(cookie){
						keyList = cookie.split(instOpts.cookieDelimiter);
						for(i=0; i<keyList.length; i++){
							node = tree.getNodeByKey(keyList[i]);
							if(node){
								if(node.selected === undefined || instOpts.overrideSource && (node.selected === false)){
	//								node.setSelected();
									node.selected = true;
									node.renderStatus();
								}
							}else{
								// node is no longer member of the tree: remove from cookie also
								local._appendKey(SELECTED, keyList[i], false);
							}
						}
					}
				}
				if(local.storeActive){
					cookie = local._data(local.cookiePrefix + ACTIVE);
					if(cookie && (opts.persist.overrideSource || !tree.activeNode)){
						node = tree.getNodeByKey(cookie);
						if(node){
							node.setActive();
						}
					}
				}
				if(local.storeFocus && prevFocus){
					node = tree.getNodeByKey(prevFocus);
					if(node){
						node.setFocus();
					}
				}
			});
		});
		// Init the tree
		return this._super(ctx);
	},
	nodeSetActive: function(ctx, flag, opts) {
		var res,
			local = this._local;

		flag = (flag !== false);
		res = this._super(ctx, flag, opts);

		if(local.storeActive){
			local._data(local.cookiePrefix + ACTIVE, this.activeNode ? this.activeNode.key : null);
		}
		return res;
	},
	nodeSetExpanded: function(ctx, flag, opts) {
		var res,
			node = ctx.node,
			local = this._local;

		flag = (flag !== false);
		res = this._super(ctx, flag, opts);

		if(local.storeExpanded){
			local._appendKey(EXPANDED, node.key, flag);
		}
		return res;
	},
	nodeSetFocus: function(ctx, flag) {
		var res,
			local = this._local;

		flag = (flag !== false);
		res = this._super(ctx, flag);

		if(flag && local.storeFocus){
			local._data(local.cookiePrefix + FOCUS, this.focusNode ? this.focusNode.key : null);
		}
		return res;
	},
	nodeSetSelected: function(ctx, flag) {
		var res,
			node = ctx.node,
			local = this._local;

		flag = (flag !== false);
		res = this._super(ctx, flag);

		if(local.storeSelected){
			local._appendKey(SELECTED, node.key, flag);
		}
		return res;
	}
});
}(jQuery, window, document));
