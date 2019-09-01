# @builtin "string.ne"
Expression -> Term                    {% (d) => d[0] %}

Term
  -> Something _ LogicOperator _ Term {% (d) => [d[0], d[2], ...d[4]]  %}
  | Something _ Term                  {% (d) => [d[0], ...d[2]] %}
  | Parenthesis                       {% (d) => d[0] %}
  | Something

Parenthesis
  -> OpenP Term CloseP                {% (d) => [d[1]] %}
  |  OpenP Something _ LogicOperator _ Term CloseP _ LogicOperator _ Term
                                      {% (d) => [[d[1], d[3], ...d[5]], d[8], ...d[10]] %}
  |  OpenP Something _ LogicOperator _ Term CloseP _ Term
                                      {% (d) => [[d[1], d[3], ...d[5]], ...d[8]] %}

OpenP
  -> "("

CloseP
  -> ")"

LogicOperator
  -> AndOperator                      {% (d) => d[0] %}
  | OrOperator                        {% (d) => d[0] %}

AndOperator
  -> "AND"                            {% (d) => d[0] %}

OrOperator
  -> "OR"                             {% (d) => d[0] %}

Something
  -> SomethingWithoutSpaces           {% (d) => d[0] %}
  | SomethingWithoutSpaces _          {% (d) => d[0] %}
  | _ SomethingWithoutSpaces          {% (d) => d[1] %}
  | _ SomethingWithoutSpaces _        {% (d) => d[1] %}

SomethingWithoutSpaces
  -> Facet                            {% (d) => d[0] %}
  | EntityName                       {% (d) => d[0] %}

Facet
  -> FacetName FacetSeparator FacetValue
                                      {% (d) => ({
                                        facetName: d[0],
                                        facetValue: d[2]
                                      }) %}

FacetSeparator
  -> ":"

FacetName
  -> String                           {% (d) => d[0] %}

FacetValue
  -> String                           {% (d) => d[0] %}

EntityName
  -> String                           {% (d, l, reject) => {
                                        if (d[0] === 'AND'
                                          || d[0] === 'OR'
                                          || d[0].indexOf(':') >= 0) {
                                          return reject;
                                        }
                                        return { dataset: d[0] };
                                      } %}

String
  -> [a-zA-Z\._\-0-9\*{}/\\=]:+           {% (d) => d[0].join("") %}

_
  -> [\s]:+                           {%  (d) => null %}
