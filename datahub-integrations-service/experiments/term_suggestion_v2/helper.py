import csv
import json
import pathlib
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import dotenv

current_dir = pathlib.Path(__file__).parent.resolve()
sys.path.append(str(current_dir.parent))

from docs_generation.graph_helper import create_datahub_graph
from pydantic import BaseModel

from datahub_integrations.gen_ai.term_suggestion_v2 import TermSuggestionBundle

assert create_datahub_graph is not None  # re-export

dotenv.load_dotenv(current_dir / "../../.env")


class SerializedResponse(BaseModel):
    response_dict: List[
        Tuple[
            str,
            str,
            Optional[List[TermSuggestionBundle]],
            Optional[Dict[str, List[TermSuggestionBundle]]],
        ]
    ]


def write_llm_output_to_csv(
    llm_response: list, csv_path: str | pathlib.Path = ""
) -> None:
    serialized_responses = SerializedResponse(response_dict=llm_response).dict()[
        "response_dict"
    ]
    if csv_path == "":
        csv_path = (
            f"term_suggestion_output_{datetime.now().strftime('%m-%d-%Y_%H-%M-%S')}.csv"
        )
    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        csvwriter = csv.writer(csvfile)

        if len(serialized_responses[0]) == 4:
            csvwriter.writerow(
                ["urn", "instance", "table_glossary_terms", "column_glossary_terms"]
            )
        else:
            csvwriter.writerow(["urn", "instance", "raw_output"])

        for row in serialized_responses:
            row = list(row)
            if len(row) == 4:
                row[3] = json.dumps(row[3], indent=2)
            csvwriter.writerow(row)
    print(f"csv file {csv_path} created successfully")
