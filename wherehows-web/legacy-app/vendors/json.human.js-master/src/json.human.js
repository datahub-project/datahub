/*globals define, module, require, document*/
(function (root, factory) {
    "use strict";
    if (typeof define === 'function' && define.amd) {
        define([], factory);
    } else if (typeof module !== 'undefined' && module.exports) {
        module.exports = factory();
    } else {
        root.JsonHuman = factory();
    }
}(this, function () {
    "use strict";

    var indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

    function makePrefixer(prefix) {
        return function (name) {
            return prefix + "-" + name;
        };
    }

    function isArray(obj) {
        return toString.call(obj) === '[object Array]';
    }

    function sn(tagName, className, data) {
        var result = document.createElement(tagName);

        result.className = className;
        result.appendChild(document.createTextNode("" + data));

        return result;
    }

    function scn(tagName, className, child) {
        var result = document.createElement(tagName),
            i, len;

        result.className = className;

        if (isArray(child)) {
            for (i = 0, len = child.length; i < len; i += 1) {
                result.appendChild(child[i]);
            }
        } else {
            result.appendChild(child);
        }

        return result;
    }

    function linkNode(child, href, target){
        var a = scn("a", HYPERLINK_CLASS_NAME, child);
        a.setAttribute('href', href);
        a.setAttribute('target', target);
        return a;
    }

    var toString = Object.prototype.toString,
        prefixer = makePrefixer("jh"),
        p = prefixer,
        ARRAY = 1,
        BOOL = 2,
        INT = 3,
        FLOAT = 4,
        STRING = 5,
        OBJECT = 6,
        FUNCTION = 7,
        UNK = 99,

        STRING_CLASS_NAME = p("type-string"),
        STRING_EMPTY_CLASS_NAME = p("type-string") + " " + p("empty"),

        BOOL_TRUE_CLASS_NAME = p("type-bool-true"),
        BOOL_FALSE_CLASS_NAME = p("type-bool-false"),
        BOOL_IMAGE = p("type-bool-image"),
        INT_CLASS_NAME = p("type-int") + " " + p("type-number"),
        FLOAT_CLASS_NAME = p("type-float") + " " + p("type-number"),

        OBJECT_CLASS_NAME = p("type-object"),
        OBJ_KEY_CLASS_NAME = p("key") + " " + p("object-key"),
        OBJ_VAL_CLASS_NAME = p("value") + " " + p("object-value"),
        OBJ_EMPTY_CLASS_NAME = p("type-object") + " " + p("empty"),

        FUNCTION_CLASS_NAME = p("type-function"),

        ARRAY_KEY_CLASS_NAME = p("key") + " " + p("array-key"),
        ARRAY_VAL_CLASS_NAME = p("value") + " " + p("array-value"),
        ARRAY_CLASS_NAME = p("type-array"),
        ARRAY_EMPTY_CLASS_NAME = p("type-array") + " " + p("empty"),

        HYPERLINK_CLASS_NAME = p('a'),

        UNKNOWN_CLASS_NAME = p("type-unk");

    function getType(obj) {
        var type = typeof obj;

        switch (type) {
            case "boolean":
                return BOOL;
            case "string":
                return STRING;
            case "number":
                return (obj % 1 === 0) ? INT : FLOAT;
            case "function":
                return FUNCTION;
            default:
                if (isArray(obj)) {
                    return ARRAY;
                } else if (obj === Object(obj)) {
                    return OBJECT;
                } else {
                    return UNK;
                }
        }
    }

    function _format(data, options, parentKey) {

        var result, container, key, keyNode, valNode, len, childs, tr, value,
            isEmpty = true,
            accum = [],
            type = getType(data);

        // Initialized & used only in case of objects & arrays
        var hyperlinksEnabled, aTarget, hyperlinkKeys ;

        switch (type) {
            case BOOL:
                var boolOpt = options.bool;
                container = document.createElement('div');

                if(boolOpt.showImage) {
                    var img = document.createElement('img');
                    img.setAttribute('class', BOOL_IMAGE);

                    img.setAttribute('src',
                        '' + (data ? boolOpt.img.true : boolOpt.img.false));

                    container.appendChild(img);
                }

                if(boolOpt.showText){
                    container.appendChild(data ?
                        sn("span", BOOL_TRUE_CLASS_NAME, boolOpt.text.true) :
                        sn("span", BOOL_FALSE_CLASS_NAME, boolOpt.text.false));
                }

                result = container;
                break;

            case STRING:
                if (data === "") {
                    result = sn("span", STRING_EMPTY_CLASS_NAME, "(Empty Text)");
                } else {
                    result = sn("span", STRING_CLASS_NAME, data);
                }
                break;
            case INT:
                result = sn("span", INT_CLASS_NAME, data);
                break;
            case FLOAT:
                result = sn("span", FLOAT_CLASS_NAME, data);
                break;
            case OBJECT:
                childs = [];

                aTarget =  options.hyperlinks.target;
                hyperlinkKeys = options.hyperlinks.keys;

                // Is Hyperlink Key
                hyperlinksEnabled =
                    options.hyperlinks.enable &&
                    hyperlinkKeys &&
                    hyperlinkKeys.length > 0;

                for (key in data) {
                    isEmpty = false;

                    value = data[key];

                    valNode = _format(value, options, key);
                    keyNode = sn("th", OBJ_KEY_CLASS_NAME, key);

                    if( hyperlinksEnabled &&
                        typeof(value) === 'string' &&
                        indexOf.call(hyperlinkKeys, key) >= 0){

                        valNode = scn("td", OBJ_VAL_CLASS_NAME, linkNode(valNode, value, aTarget));
                    } else {
                        valNode = scn("td", OBJ_VAL_CLASS_NAME, valNode);
                    }

                    tr = document.createElement("tr");
                    tr.appendChild(keyNode);
                    tr.appendChild(valNode);

                    childs.push(tr);
                }

                if (isEmpty) {
                    result = sn("span", OBJ_EMPTY_CLASS_NAME, "(Empty Object)");
                } else {
                    result = scn("table", OBJECT_CLASS_NAME, scn("tbody", '', childs));
                }
                break;
            case FUNCTION:
                result = sn("span", FUNCTION_CLASS_NAME, data);
                break;
            case ARRAY:
                if (data.length > 0) {
                    childs = [];
                    var showArrayIndices = options.showArrayIndex;

                    aTarget =  options.hyperlinks.target;
                    hyperlinkKeys = options.hyperlinks.keys;

                    // Hyperlink of arrays?
                    hyperlinksEnabled = parentKey && options.hyperlinks.enable &&
                        hyperlinkKeys &&
                        hyperlinkKeys.length > 0 &&
                        indexOf.call(hyperlinkKeys, parentKey) >= 0;

                    for (key = 0, len = data.length; key < len; key += 1) {

                        keyNode = sn("th", ARRAY_KEY_CLASS_NAME, key);
                        value = data[key];

                        if(hyperlinksEnabled && typeof(value) === "string") {
                            valNode = _format(value, options, key);
                            valNode = scn("td", ARRAY_VAL_CLASS_NAME, linkNode(valNode, value, aTarget));
                        } else {
                            valNode = scn("td", ARRAY_VAL_CLASS_NAME, _format(value, options, key));
                        }

                        tr = document.createElement("tr");

                        if(showArrayIndices) {
                            tr.appendChild(keyNode);
                        }
                        tr.appendChild(valNode);

                        childs.push(tr);
                    }

                    result = scn("table", ARRAY_CLASS_NAME, scn("tbody", '', childs));
                } else {
                    result = sn("span", ARRAY_EMPTY_CLASS_NAME, "(Empty List)");
                }
                break;
            default:
                result = sn("span", UNKNOWN_CLASS_NAME, data);
                break;
        }

        return result;
    }

    function format(data, options) {
        options = validateOptions(options || {});

        var result;

        result = _format(data, options);
        result.className = result.className + " " + prefixer("root");

        return result;
    }

    function validateOptions(options){
        options = validateArrayIndexOption(options);
        options = validateHyperlinkOptions(options);
        options = validateBoolOptions(options);

        // Add any more option validators here

        return options;
    }


    function validateArrayIndexOption(options) {
        if(options.showArrayIndex === undefined){
            options.showArrayIndex = true;
        } else {
            // Force to boolean just in case
            options.showArrayIndex = options.showArrayIndex ? true: false;
        }

        return options;
    }

    function validateHyperlinkOptions(options){
        var hyperlinks = {
            enable : false,
            keys : null,
            target : ''
        };

        if(options.hyperlinks && options.hyperlinks.enable) {
            hyperlinks.enable = true;

            hyperlinks.keys =  isArray(options.hyperlinks.keys) ? options.hyperlinks.keys : [];

            if(options.hyperlinks.target) {
                hyperlinks.target = '' + options.hyperlinks.target;
            } else {
                hyperlinks.target = '_blank';
            }
        }

        options.hyperlinks = hyperlinks;

        return options;
    }

    function validateBoolOptions(options){
        if(!options.bool){
            options.bool = {
                text:  {
                    true : "true",
                    false : "false"
                },
                img : {
                    true: "",
                    false: ""
                },
                showImage : false,
                showText : true
            };
        } else {
            var boolOptions = options.bool;

            // Show text if no option
            if(!boolOptions.showText && !boolOptions.showImage){
                boolOptions.showImage  =  false;
                boolOptions.showText  =  true;
            }

            if(boolOptions.showText){
                if(!boolOptions.text){
                    boolOptions.text = {
                        true : "true",
                        false : "false"
                    };
                } else {
                    var t = boolOptions.text.true, f = boolOptions.text.false;

                    if(getType(t) != STRING || t === ''){
                        boolOptions.text.true = 'true';
                    }

                    if(getType(f) != STRING || f === ''){
                        boolOptions.text.false = 'false';
                    }
                }
            }

            if(boolOptions.showImage){
                if(!boolOptions.img.true && !boolOptions.img.false){
                    boolOptions.showImage = false;
                }
            }
        }

        return options;
    }

    return {
        format: format
    };
}));
