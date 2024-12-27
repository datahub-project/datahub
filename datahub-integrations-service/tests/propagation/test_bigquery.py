import pytest

from datahub_integrations.propagation.bigquery.util import BigqueryTagHelper


class TestBigqueryTagHelper:

    def test_truncate_policy_tag_name_no_truncation(self) -> None:
        input_str = "a__b__c"
        result = BigqueryTagHelper.truncate_policy_tag_name(input_str)
        assert result == input_str

    def test_truncate_policy_tag_name_truncate(self) -> None:
        input_str = "a__b__c__d__e__f__g__h__i__j__k__l__m__n__o__p__q__r__s__t__u__v__w__x__y__z"
        result = BigqueryTagHelper.truncate_policy_tag_name(input_str, max_length=20)
        assert result == "x__y__z__3c019a7e2c"

    def test_truncate_policy_tag_name_minimum_characters(self) -> None:
        input_str = "a__b__c__d__e__f__g__h__i__j__k__l__m__n__o__p__q__r__s__t__u__v__w__x__y__z"
        result = BigqueryTagHelper.truncate_policy_tag_name(input_str, max_length=13)
        assert result == "z__594e519ae4"

    def test_truncate_policy_tag_name_truncate_end(self) -> None:
        input_str = "a__b__c__d__e__f__g__h__i__j__k__l__m__n__o__p__q__r__s__t__u__v__w__x__y__z"
        with pytest.raises(ValueError):
            BigqueryTagHelper.truncate_policy_tag_name(input_str, max_length=10)

    def test_truncate_policy_tag_name_exact_length(self) -> None:
        input_str = "a__b__c__d__e__f__g__h__i__j__k__l__m__n__o__p__q__r__s__t__u__v__w__x__y__z"
        result = BigqueryTagHelper.truncate_policy_tag_name(
            input_str, max_length=len(input_str)
        )
        assert result == input_str

    def test_truncate_policy_tag_name_without_double_underscores(self) -> None:
        input_str = "my_test_string"
        result = BigqueryTagHelper.truncate_policy_tag_name(
            input_str, max_length=len(input_str)
        )
        assert result == input_str

    def test_truncate_policy_tag_name_empty_string(self) -> None:
        input_str = ""
        result = BigqueryTagHelper.truncate_policy_tag_name(input_str)
        assert result == input_str
