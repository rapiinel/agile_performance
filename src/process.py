"""Python script to process the data"""

import re

import joblib
import numpy as np
import pandas as pd
from prefect import flow, task
from sklearn.model_selection import train_test_split

from config import JiraAccountParams, Location, ProcessConfig
from jira_api import lmjira


@task
def get_raw_data(jquery: str, team_list: list[str] = []):
    """Read raw data

    Parameters
    ----------
    jquery : str
        jira query language similar to sql
    team_list: list[str]
        list of all active team that you want the data to be pulled.
        if team_list is greater than 0, the jquery variable will be ignored
    """
    temp_df = []
    change_log = []
    if len(team_list) != 0:
        for team in team_list:
            print(team)
            data = lmjira(
                f'"Team[Dropdown]" = "{team}" AND created >= 2023-01-01 order by created DESC',
                0,
            )  # project = LMS AND AND created >= -30d order by created DESC
            data.multisearch()
            data.df["team"] = team
            temp_df.append(data.df)
            change_log.append(data.changelog)
        df = pd.concat(temp_df)
        changelog = pd.concat(change_log)
        return df, changelog
    else:
        data = lmjira(jquery, 5)
        data.multisearch()
        return data.df, data.changelog


@task
def datatype_todate(data: pd.DataFrame, columns: list):
    """Convert date columns to datetime format in pandas"""
    for column in columns:
        data[column] = pd.to_datetime(data[column])
        data[column] = [x.date() for x in data[column]]
    return data


def get_sprint(str_value):
    str_value = str(str_value).lower()
    if "." in str_value:
        result = re.findall(r"sprint\d+.\d+|sprint \d+.\d+", str_value)
    else:
        result = re.findall(r"sprint \d+|sprint\d+", str_value)
    if len(result) > 0:
        result = result[0]
        for value in ["sprint ", "sprint"]:
            result = result.replace(value, "")
        return "sprint " + result
    else:
        return np.nan


@task
def drop_columns(data: pd.DataFrame, columns: list):
    """Drop unimportant columns

    Parameters
    ----------
    data : pd.DataFrame
        Data to process
    columns : list
        Columns to drop
    """
    return data.drop(columns=columns)


def get_ticket_value(row: pd.Series):
    """Tag tickets that was identified with value for the stakeholders"""
    variables = [
        "RCA - Issue Classification",
        "RCA - Causing Ticket",
        "Fix versions",
        "Release Date (Actual)",
        "Release Date (Estimated - Original)",
        "Release Date (Estimated - Current)",
    ]
    if str(row["issue type"]) in ["Story", "Bug"]:
        return 1
    for variable in variables:
        if str(row[variable]) != "None":
            return 1
        else:
            return np.nan


def get_status(row: pd.Series):
    """Tag tickets that are complete in jira since it has multiple types"""
    status_list = [
        "Release Preparation",
        "Pending Release",
        "Closed",
        "Post-Release Monitoring",
        "Launched",
        "Data gathering",
        "Closed",
        "Data Gathering",
    ]
    if str(row["status"]) in status_list:
        return 1


def get_bug(row: pd.Series):
    """Tag critical bugs"""
    bug_indicator = ["RCA - Issue Classification", "RCA - Causing Ticket"]
    # major concern is post-release-bug #'existing-bug','enhancement','defect',
    bug_status = ["post-release-bug"]
    if str(row["RCA - Causing Ticket"]) != "None":
        for bug in bug_indicator:
            if str(row[bug]) != "None":
                if str(row["RCA - Issue Classification"]) in bug_status:
                    return 1
            else:
                return np.nan
    else:
        return np.nan


@task
def get_X_y(data: pd.DataFrame, label: str):
    """Get features and label

    Parameters
    ----------
    data : pd.DataFrame
        Data to process
    label : str
        Name of the label
    """
    X = data.drop(columns=label)
    y = data[label]
    return X, y


@task
def split_train_test(X: pd.DataFrame, y: pd.DataFrame, test_size: int):
    """_summary_

    Parameters
    ----------
    X : pd.DataFrame
        Features
    y : pd.DataFrame
        Target
    test_size : int
        Size of the test set
    """
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=0
    )
    return {
        "X_train": X_train,
        "X_test": X_test,
        "y_train": y_train,
        "y_test": y_test,
    }


@task
def save_processed_data(data: dict, save_location: str):
    """Save processed data

    Parameters
    ----------
    data : dict
        Data to process
    save_location : str
        Where to save the data
    """
    joblib.dump(data, save_location)


@flow
def process(
    location: Location = Location(),
    config: ProcessConfig = ProcessConfig(),
    jiraparams: JiraAccountParams = JiraAccountParams(),
):
    """Flow to process the ata

    Parameters
    ----------
    location : Location, optional
        Locations of inputs and outputs, by default Location()
    config : ProcessConfig, optional
        Configurations for processing data, by default ProcessConfig()
    """

    data, changelog = get_raw_data(
        jiraparams.sample_jquery, jiraparams.team_list
    )
    data.to_csv(location.data_raw + "raw_data.csv", index=False)
    changelog.to_csv(location.data_raw + "raw_changelog.csv", index=False)

    data = data.explode("sprint").dropna(subset=["sprint"])
    data = datatype_todate(data, jiraparams.date_columns)
    data["sprint-cleaned"] = data.apply(
        lambda x: get_sprint(x["sprint"]), axis=1
    )
    data["ticket with value?"] = data.apply(
        lambda x: get_ticket_value(x), axis=1
    )
    data["ticket done?"] = data.apply(lambda x: get_status(x), axis=1)
    data["ticket a bug?"] = data.apply(lambda x: get_bug(x), axis=1)
    data.to_csv(location.data_process + "processed_data.csv", index=False)


if __name__ == "__main__":
    process(config=ProcessConfig(test_size=0.1))
