"""Python script to process the data"""

import joblib
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
                5,
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

    # team = "this is a team variable"
    # print(jiraparams.jquery)

    data, changelog = get_raw_data(
        jiraparams.sample_jquery, jiraparams.team_list
    )
    data.to_csv(location.data_raw + "raw_data.csv", index=False)
    changelog.to_csv(location.data_raw + "raw_changelog.csv", index=False)
    # processed = drop_columns(data, config.drop_columns)
    # X, y = get_X_y(processed, config.label)
    # split_data = split_train_test(X, y, config.test_size)
    # save_processed_data(split_data, location.data_process)


if __name__ == "__main__":
    process(config=ProcessConfig(test_size=0.1))
