from .refinery import Refinery, unpack_processed_value, format_error_body
from services.data.db_utils import DBResponse


class TaskRefiner(Refinery):
    """
    Refiner class for postprocessing Task rows.

    Fetches specified content from S3 and cleans up unnecessary fields from response.

    Parameters
    -----------
    cache : AsyncCacheClient
        An instance of a cache that implements the GetArtifacts action.
    """

    def __init__(self, cache):
        super().__init__(field_names=["task_ok", "foreach_stack"], cache=cache)

    async def fetch_data(self, locations, event_stream=None, invalidate_cache=False):
        _res = await self.artifact_store.cache.GetArtifactsWithStatus(
            locations, invalidate_cache=invalidate_cache)
        if _res.has_pending_request():
            async for event in _res.stream():
                if event["type"] == "error":
                    if event_stream:
                        event_stream(event)
            await _res.wait()  # wait for results to be ready
        return _res.get() or {}  # cache get() might return None if no keys are produced.

    async def postprocess(self, response: DBResponse, invalidate_cache=False):
        """
        Calls the refiner postprocessing to fetch S3 values for content.
        Combines 'task_ok' boolean into the 'status' key. Processes the 'foreach_stack' for the relevant content.

        Parameters
        ----------
        response : DBResponse
            The DBResponse to be refined

        Returns
        -------
        A refined DBResponse, or in case of errors, the original DBResponse
        """
        if response.response_code != 200 or not response.body:
            return response
        refined_response = await self._postprocess(response, invalidate_cache=invalidate_cache)

        def _process(item):
            if item['status'] == 'unknown' and item['task_ok'] is not None:
                success, value, detail = unpack_processed_value(item['task_ok'])
                if success:
                    # cast artifact content to string if it was successfully fetched
                    # as some artifacts retain their type if they are Json serializable.
                    if value is False:
                        item['status'] = 'failed'
                    elif value is True:
                        item['status'] = 'completed'
                else:
                    item['postprocess_error'] = format_error_body(
                        value if value else "artifact-handle-failed",
                        detail if detail else "Unknown error during artifact processing"
                    )

            item.pop('task_ok', None)

            if item['foreach_stack']:
                success, value, detail = unpack_processed_value(item['foreach_stack'])
                if success:
                    if len(value) > 0 and len(value[0]) >= 4:
                        _, _name, _, _index = value[0]
                        item['foreach_label'] = "{}[{}]".format(item['task_id'], _index)
                else:
                    item['postprocess_error'] = format_error_body(
                        value if value else "artifact-handle-failed",
                        detail if detail else "Unknown error during artifact processing"
                    )

            item.pop('foreach_stack', None)
            return item

        if isinstance(refined_response.body, list):
            body = [_process(task) for task in refined_response.body]
        else:
            body = _process(refined_response.body)

        return DBResponse(response_code=refined_response.response_code, body=body)