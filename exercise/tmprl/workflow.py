import dataclasses

import temporalio.workflow


@dataclasses.dataclass
class Params:
    strings: list[str]


@dataclasses.dataclass
class Result:
    strings: list[str]


@temporalio.workflow.defn(name="Flip", sandboxed=False)
class Flip:
    @temporalio.workflow.run
    async def run(self, params: Params) -> Result:
        return Result(strings=[s[::-1] for s in params.strings])
