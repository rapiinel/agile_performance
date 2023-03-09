import numpy as np
import pandas as pd
from jira import JIRA

from config import JiraAccountParams

credentials = JiraAccountParams()


options = {"server": credentials.server}

jira = JIRA(options, basic_auth=(credentials.user, credentials.apikey))


class lmjira:
    def __init__(self, jquery, maxresult):
        """initialization"""
        self.jquery = jquery
        self.maxresult = maxresult
        self.changelog_list = []

    def multisearch(self):
        """search logic"""
        temp_dict = {
            "created datetime": [],
            "key": [],
            "issue type": [],
            "summary": [],
            "reporter": [],
            "status": [],
            "sprint": [],
            "story points": [],
            "Epic Link": [],
            "team": [],
            "time spent (s)": [],
            "Release Date (Actual)": [],
            "Release Date (Estimated - Original)": [],
            "Release Date (Estimated - Current)": [],
            "RCA - Issue Classification": [],
            "RCA - Causing Ticket": [],
            "Fix versions": [],
        }
        issues = jira.search_issues(
            jql_str=self.jquery, maxResults=self.maxresult
        )
        allfields = jira.fields()
        self.nameMap = {
            jira.field["name"]: jira.field["id"] for jira.field in allfields
        }

        for singleIssue in issues:
            print(
                "{}: {}:{}".format(
                    singleIssue.key,
                    singleIssue.fields.summary,
                    singleIssue.fields.reporter.displayName,
                )
            )

            temp_dict["created datetime"].append(singleIssue.fields.created)
            temp_dict["key"].append(singleIssue.key)
            temp_dict["summary"].append(singleIssue.fields.summary)
            # temp_dict['issue type'].append(getattr(singleIssue.fields, self.nameMap["Issue Type"]).name)
            temp_dict["issue type"].append(singleIssue.fields.issuetype)
            temp_dict["reporter"].append(
                singleIssue.fields.reporter.displayName
            )
            temp_dict["status"].append(singleIssue.fields.status)

            try:
                sprints = [
                    x.name
                    for x in getattr(
                        singleIssue.fields, self.nameMap["Sprint"]
                    )
                ]
                temp_dict["sprint"].append(sprints)
            except Exception:
                temp_dict["sprint"].append(np.nan)

            temp_dict["Epic Link"].append(
                getattr(singleIssue.fields, self.nameMap["Epic Link"])
            )
            try:
                temp_dict["story points"].append(
                    getattr(singleIssue.fields, self.nameMap["Story Points"])
                )
            except AttributeError:
                temp_dict["story points"].append(np.nan)
            temp_dict["time spent (s)"].append(
                getattr(singleIssue.fields, self.nameMap["Σ Time Spent"])
            )

            try:
                temp_dict["team"].append(
                    getattr(singleIssue.fields, self.nameMap["Team"])
                )
            except AttributeError:
                temp_dict["team"].append(np.nan)

            for value in [
                "Release Date (Actual)",
                "Release Date (Estimated - Original)",
                "Release Date (Estimated - Current)",
                "RCA - Issue Classification",
                "RCA - Causing Ticket",
                "Fix versions",
            ]:
                try:
                    temp_dict[value].append(
                        getattr(singleIssue.fields, self.nameMap[value])
                    )
                except AttributeError:
                    temp_dict[value].append(np.nan)

            self.singleIssue = singleIssue

            # getting changelogs for 1 issue
            self.get_changelog(singleIssue.key)

        self.df = pd.DataFrame.from_dict(temp_dict, orient="columns")
        if len(self.changelog_list) > 0:
            self.changelog = (
                pd.concat(self.changelog_list)
                .reset_index(drop=True)
                .drop_duplicates()
            )

    def get_changelog(self, key):
        """function to get the change log, change log in a different table. This function will be used in the multi search function"""
        issue_key = key
        issue_jql = jira.issue(key, expand="changelog")
        changelog = issue_jql.changelog
        history_dict = {}
        df_list = []

        if len(changelog.histories) > 0:
            for history in changelog.histories:
                for key in ["author", "created", "id"]:
                    history_dict[key] = getattr(history, key)
                item_dict = {
                    "field": [],
                    "fieldtype": [],
                    "from": [],
                    "fromString": [],
                    "to": [],
                    "toString": [],
                }
                for item in history.items:
                    for key in item_dict.keys():
                        item_dict[key].append(getattr(item, key))
                    temp_df_item = pd.DataFrame.from_dict(item_dict)
                    for key in history_dict.keys():
                        temp_df_item[key] = history_dict[key]
                    df_list.append(temp_df_item)
            item_df = pd.concat(df_list).reset_index(drop=True)
            item_df["key"] = issue_key
            self.changelog_list.append(item_df)


if __name__ == "__main__":
    hr = lmjira(
        'project = LMS AND "Team[Dropdown]" = "Team 5 | Avatar" AND created >= -30d order by created DESC',
        5,
    )
    hr.multisearch()
    hr.df
