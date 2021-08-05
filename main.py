from github import Github
from elasticsearch import Elasticsearch
import os

token = os.getenv('GITHUB_TOKEN')
endpoint = os.getenv('ES_ENDPOINT')

def connect_elasticsearch():
    es = None
    es = Elasticsearch(endpoint)
    if es.ping():
        print('Yay! Connected to ElasticSearch')
    else:
        print('Awww it could not connect!')
        raise ValueError("Connection failed")
    return es

def fetch_commits_and_index():
    g = Github(token)
    repo = g.get_repo("RockstarLang/rockstar")
    es = connect_elasticsearch()

    for commit in repo.get_commits():
        commit_info = repo.get_commit(commit.sha)

        data = {
            "date": commit_info.commit.author.date,
            "message": commit_info.commit.message,
            "username": getattr(commit_info.author, 'login', commit_info.commit.author.name),
            "committer": {
                "account_created": getattr(commit_info.author, 'created_at', '1970-01-01T00:00:00')
            }
        }

        response = es.index(
            index = 'commits',
            doc_type = 'commit',
            id = commit.sha,
            body = data
        )

        print('Document for commit '+commit.sha+' been '+response['result'])

if __name__ == "__main__":
    print('Starting')

    fetch_commits_and_index()

    print('Finished')
