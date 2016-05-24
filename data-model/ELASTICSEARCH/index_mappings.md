curl -XPUT '$YOUR_INDEX_URL:9200/wherehows' -d '
{
  "mappings": {
    "dataset": {},
    "comment": {
      "_parent": {
        "type": "dataset"
      }
    },
    "field": {
      "_parent": {
        "type": "dataset"
      }
    }
  }
}
'

curl -XPUT '$YOUR_INDEX_URL:9200/wherehows/flow_jobs/_mapping' -d '
{
  "flow_jobs": {
    "properties": {
      "jobs": {
        "type": "nested",
        "properties": {
          "job_name":    { "type": "string"  },
          "job_path": { "type": "string"  },
          "job_type": { "type": "string"  },
          "pre_jobs": { "type": "string"  },
          "post_jobs": { "type": "string"  },
          "is_current": { "type": "string"  },
          "is_first": { "type": "string"  },
          "is_last": { "type": "string"  },
          "job_type_id": { "type": "short"   },
      "app_id": { "type": "short"   },
      "flow_id": { "type": "long"   },
      "job_id": { "type": "long"   }
        }
      }
    }
  }
}
'