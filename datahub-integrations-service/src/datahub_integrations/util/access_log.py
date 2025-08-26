import time
from datetime import datetime
from typing import Any

import fastapi
from loguru import logger


async def access_log(request: fastapi.Request, call_next: Any) -> fastapi.Response:
    start_time = time.perf_counter()
    response: fastapi.Response = await call_next(request)

    end_time = time.perf_counter()
    process_time = end_time - start_time

    log_time = datetime.now()
    logger.opt(raw=True, colors=True).info(
        "<green>{}</green> | {} <b>{} <blue>{}</blue></b> => <yellow>{}</yellow> <magenta>{:.2f}ms</magenta>\n".format(
            str(log_time)[:-3],
            (
                f"{request.client.host}:{request.client.port}"
                if request.client
                else "unknown"
            ),
            request.method,
            request.url.path,
            response.status_code,
            process_time * 1000,
        )
    )

    return response
