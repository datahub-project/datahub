import { graphToDot } from '@datahub/shared/utils/graph/graph-to-dot';
import { module, test } from 'qunit';
import { simpleGraph } from '../../../../tests/helpers/graph/graphs';
import { moveAttributeEdgesToEdges } from '@datahub/shared/utils/graph/graph-transformations';

module('Unit | Utility | graph-to-dot', function(_hooks): void {
  test('it works', function(assert): void {
    const result = graphToDot(moveAttributeEdgesToEdges(simpleGraph));
    assert.equal(
      result.trim().replace(/\s+/gi, ' '),
      `
        digraph {
          graph [
            rankdir = \"LR\"
            bgcolor = \"none\"
          ];
          node [
            fontsize = \"16\"
            fontname = \"helvetica, open-sans\"
            shape = \"none\"
            margin = \"0\"
          ];
          edge [];
          ranksep = 2
          \"id1\" [
            id = \"ENTITY::id1\"
            label = <<TABLE CELLSPACING=\"10\" BORDER=\"0\"><TR ><TD ><TABLE CELLSPACING=\"0\" BORDER=\"0\" CELLPADDING=\"0\"><TR ><TD ALIGN=\"LEFT\" ID=\"ENTITY-TITLE::id1\" HREF=\"-\" CELLPADDING=\"10\" CELLSPACING=\"0\" BGCOLOR=\"red\" PORT=\"root\"><FONT POINT-SIZE=\"20\">displayName1</FONT></TD></TR><TR ><TD PORT=\"attribute\" ID=\"ENTITY-ATTRIBUTE::attribute\" HREF=\"-\" BGCOLOR=\"red\"><TABLE CELLSPACING=\"0\" BORDER=\"0\" CELLPADDING=\"10\"><TR ><TD ALIGN=\"LEFT\">attribute</TD><TD ALIGN=\"RIGHT\">string</TD></TR></TABLE></TD></TR><TR ><TD PORT=\"reference\" ID=\"ENTITY-ATTRIBUTE::reference::id2\" HREF=\"-\" BGCOLOR=\"red\"><TABLE CELLSPACING=\"0\" BORDER=\"0\" CELLPADDING=\"10\"><TR ><TD ALIGN=\"LEFT\">reference</TD><TD ALIGN=\"RIGHT\">DisplayName2</TD></TR></TABLE></TD></TR></TABLE></TD></TR></TABLE>>
          ]
          \"id2\" [
            id = \"ENTITY::id2\"
            label = <<TABLE CELLSPACING=\"10\" BORDER=\"0\"><TR ><TD ><TABLE CELLSPACING=\"0\" BORDER=\"0\" CELLPADDING=\"0\"><TR ><TD ALIGN=\"LEFT\" ID=\"ENTITY-TITLE::id2\" HREF=\"-\" CELLPADDING=\"10\" CELLSPACING=\"0\" BGCOLOR=\"red\" PORT=\"root\"><FONT POINT-SIZE=\"20\">displayName2</FONT></TD></TR><TR ><TD PORT=\"attribute\" ID=\"ENTITY-ATTRIBUTE::attribute\" HREF=\"-\" BGCOLOR=\"red\"><TABLE CELLSPACING=\"0\" BORDER=\"0\" CELLPADDING=\"10\"><TR ><TD ALIGN=\"LEFT\">attribute</TD><TD ALIGN=\"RIGHT\">string</TD></TR></TABLE></TD></TR></TABLE></TD></TR></TABLE>>
          ]
          \"id1\":reference -> \"id2\":root  [
            id = \"EDGE::id1::reference::id2\"
            label = \"displayName1:reference\"
          ]
        }`
        .trim()
        .replace(/\s+/gi, ' ')
    );
  });
});
