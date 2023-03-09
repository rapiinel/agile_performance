"""
create Pydantic models
"""
from typing import List

from pydantic import BaseModel, validator


def must_be_non_negative(v: float) -> float:
    """Check if the v is non-negative

    Parameters
    ----------
    v : float
        value

    Returns
    -------
    float
        v

    Raises
    ------
    ValueError
        Raises error when v is negative
    """
    if v < 0:
        raise ValueError(f"{v} must be non-negative")
    return v


class Location(BaseModel):
    """Specify the locations of inputs and outputs"""

    folder_link: str = ""
    data_raw: str = "data/raw/"
    data_process: str = "data/processed/"
    data_final: str = "data/final/"
    model: str = "models/"
    input_notebook: str = "notebooks/analyze_results.ipynb"
    output_notebook: str = "notebooks/results.ipynb"


class ProcessConfig(BaseModel):
    """Specify the parameters of the `process` flow"""

    drop_columns: List[str] = ["Id"]
    label: str = "Species"
    test_size: float = 0.3

    _validated_test_size = validator("test_size", allow_reuse=True)(
        must_be_non_negative
    )


class ModelParams(BaseModel):
    """Specify the parameters of the `train` flow"""

    C: List[float] = [0.1, 1, 10, 100, 1000]
    gamma: List[float] = [1, 0.1, 0.01, 0.001, 0.0001]

    _validated_fields = validator("*", allow_reuse=True, each_item=True)(
        must_be_non_negative
    )


class JiraAccountParams(BaseModel):
    """Specify the parameters of the jira account and other queries"""

    user: str = "raffie.navaluna@legalmatch.com"
    apikey: str = "ATATT3xFfGF0qWVjYCKxk31Tut32YsjIEvuyvD9nJu0nufnXZdkzvN9gsMWiZ7LrRdhnjAO1cmc5C2G5PtHb_t3FfXQrXjD4VQWhRk6E6GZalOmBAonnEUfv890pEiGCtqDT8OQwg_Ru5EVszuRRk3zIk7rvRdLxHqGq94RNyfclY3rDaXeV9gY=5E104671"
    server: str = "https://legalmatch.atlassian.net"
