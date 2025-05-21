# DSP Datahub Airflow Plugin

Datahub Airflow Plugin 팀 현황에 맞게 수정한 커스텀 플러그인

### 플러그인 수정 내용

- Dataset Converter
    - Datahub 기반의 Dataset Converter 개발
    - Airflow, OpenLineage, Datahub Dataset -> Datahub Dataset 변환
- SQL Parsing 을 Hive base 로 변경
    - datahub_sql_parser 의 경우 Airflow Connection Type 기반으로 지정된 dialect 에 대해서만 파싱이 가능한데, impala, kudu(?) 는 지원하지 않음
    - 특정 쿼리엔진에서만 지원하는 키워드 외에는 hive 쿼리와 동일하기 때문에 쿼리 파싱 베이스를 hive 로 변경
- platform_instance
    - DDH01, DDH02 등 같은 테이블의 다른 클러스터 구분을 위해 platform_instance 추가
- SQL Query Parsing
    - 파이썬 문자열 포맷팅 변경
    - `%(load_date)s` -> `:load_data` 와 같은 형태로 변환

## 빌드 / 배포

```
scripts/release.sh 버전 수정 후 배포
```

# Datahub Airflow Plugin

See [the DataHub Airflow docs](https://docs.datahub.com/docs/lineage/airflow) for details.

## Developing

See the [developing docs](../../metadata-ingestion/developing.md).
