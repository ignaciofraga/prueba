# -*- coding: utf-8 -*-
"""
Created on Thu Jul 28 11:56:39 2022

@author: ifraga
"""

# Carga de módulos

import psycopg2
import sqlite3
from sqlalchemy import create_engine
import pandas



# BASE DE DATOS POSTGRESQL 
base_datos_postgresql = 'IEO_Coruna'
usuario_postgresql    = 'postgres'
contrasena_postgresql = 'IEO2022'
puerto_postgresql     = '5432'

# GEMELO SQLITE (Réplica)
base_datos_sqlite  = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/COAC.db'

# import base64
from github import Github
# from github import InputGitTreeElement
#import github

#from github.MainClass import Github, GithubIntegration

user = "ignaciofraga"
password = "Nacho_1985"
g = Github(user,password)

# user2 = g.get_user()
# user2 = g.get_user(user)
# repo = g.get_repo("ignaciofraga/prueba")
# repo.name
# #repo = g.get_repo("PyGithub/PyGithub")
# # repo.name

repo = g.get_repo("ignaciofraga/prueba") # repo name
#repo = g.get_user().get_repo("ignaciofraga/prueba") # repo name
file_list = [base_datos_sqlite]
file_names = ['REPLICA_COAC.db']

contents = repo.get_contents("")
for content_file in contents:
    print(content_file)

import base64
contents = repo.get_contents("/requirements.txt", ref="main")
repo.update_file('/requirements.txt', 'lele','texto', contents.sha)

#update_file(path, message, content, sha, branch=NotSet, committer=NotSet, author=NotSet)

# import github
# g = github.Github(user, password)
# repo = g.get_user().get_repo("ignaciofraga/prueba")
# file = repo.get_file_contents("/your_file.txt")

#repo.create_file('/test', 'commit message', 'content of the file', branch='main')
#repo.update_file('/requirements.txt','actualizado requirements','nuevo')
# contents = repo.get_file_contents("requirements.txt")
# print(contents)

#repo.create_file("test.txt", "ignaciofraga", "prueba")

#repo.create_file("test.txt", "test", "test", branch="main")


# contents = repo.get_contents("COAC.db")
# print(contents)

# all_files = []
# contents = repo.get_contents("")
# while contents:
#     file_content = contents.pop(0)
#     if file_content.type == "dir":
#         contents.extend(repo.get_contents(file_content.path))
#     else:
#         file = file_content
#         all_files.append(str(file).replace('ContentFile(path="','').replace('")',''))

# # with open(base_datos_sqlite, 'r') as file:
# #     content = file.read()


# import base64
# #ruta_archivo = 
# with open(base_datos_sqlite, 'r') as file:
#     content = base64.b64encode(file)

# # Upload to github
# git_prefix = 'DATOS/'
# git_file = git_prefix + 'COAC.db'
# if git_file in all_files:
#     contents = repo.get_contents(git_file)
#     repo.update_file(contents.path, "committing files", content, contents.sha, branch="main")
#     print(git_file + ' UPDATED')
# else:
#     repo.create_file(git_file, "committing files", content, branch="main")
#     print(git_file + ' CREATED')






# repo.update_file(contents.path, "more tests", "more tests", contents.sha, branch="main")


# repo = g.get_repo("PyGithub/PyGithub")
#contents = repo.get_contents("COAC.db", ref="test")
#repo.update_file(contents.path, "more tests", "more tests", contents.sha, branch="main")


# import requests
# import base64
# import json
# import datetime


# #def push_to_repo_branch(gitHubFileName, fileName, repo_slug, branch, user, token):
# '''
# Push file update to GitHub repo

# :param gitHubFileName: the name of the file in the repo
# :param fileName: the name of the file on the local branch
# :param repo_slug: the github repo slug, i.e. username/repo
# :param branch: the name of the branch to push the file to
# :param user: github username
# :param token: github user token
# :return None
# :raises Exception: if file with the specified name cannot be found in the repo
# '''

# repo_slug = "ignaciofraga/prueba"
# branch    = "master"
# user = "ignaciofraga"
# token = "Nacho_1985" 
# gitHubFileName = "DATOS/COAC.db"
# fileName = base_datos_sqlite

# message = "Automated update " + str(datetime.datetime.now())
# path = "https://api.github.com/repos/%s/branches/%s" % (repo_slug, branch)

# r = requests.get(path, auth=(user,token))
# if not r.ok:
#     print("Error when retrieving branch info from %s" % path)
#     print("Reason: %s [%d]" % (r.text, r.status_code))
#     raise
# rjson = r.json()
# treeurl = rjson['commit']['commit']['tree']['url']
# r2 = requests.get(treeurl, auth=(user,token))
# if not r2.ok:
#     print("Error when retrieving commit tree from %s" % treeurl)
#     print("Reason: %s [%d]" % (r2.text, r2.status_code))
#     raise
# r2json = r2.json()
# sha = None

# for file in r2json['tree']:
#     # Found file, get the sha code
#     if file['path'] == gitHubFileName:
#         sha = file['sha']

# # if sha is None after the for loop, we did not find the file name!
# if sha is None:
#     print ("Could not find " + gitHubFileName + " in repos 'tree' ")
#     raise Exception

# with open(fileName) as data:
#     content = base64.b64encode(data.read())

# # gathered all the data, now let's push
# inputdata = {}
# inputdata["path"] = gitHubFileName
# inputdata["branch"] = branch
# inputdata["message"] = message
# inputdata["content"] = content
# if sha:
#     inputdata["sha"] = str(sha)

# #updateURL = "https://api.github.com/repos/EBISPOT/RDF-platform/contents/" + gitHubFileName
# updateURL = "https://api.github.com/repos/ignaciofraga/contents/" + gitHubFileName
# try:
#     rPut = requests.put(updateURL, auth=(user,token), data = json.dumps(inputdata))
#     if not rPut.ok:
#         print("Error when pushing to %s" % updateURL)
#         print("Reason: %s [%d]" % (rPut.text, rPut.status_code))
#         raise Exception
# except requests.exceptions.RequestException as e:
#     print ('Something went wrong! I will print all the information that is available so you can figure out what happend!')
#     print (rPut)
#     print (rPut.headers)
#     print (rPut.text)
#     print (e)

















# all_files = []
# contents = repo.get_contents("")
# while contents:
#     file_content = contents.pop(0)
#     if file_content.type == "dir":
#         contents.extend(repo.get_contents(file_content.path))
#     else:
#         file = file_content
#         all_files.append(str(file).replace('ContentFile(path="','').replace('")',''))

# # with open(base_datos_sqlite, 'r') as file:
# #     content = file.read()

# with open(base_datos_sqlite) as data:
#     content = base64.b64encode(data.read())

# # Upload to github
# git_prefix = 'DATOS/'
# git_file = git_prefix + 'COAC.db'
# if git_file in all_files:
#     contents = repo.get_contents(git_file)
#     repo.update_file(contents.path, "committing files", content, contents.sha, branch="master")
#     print(git_file + ' UPDATED')
# else:
#     repo.create_file(git_file, "committing files", content, branch="master")
#     print(git_file + ' CREATED')







# commit_message = 'python commit'
# # master_ref = repo.get_git_ref('heads/master')
# master_ref = repo.get_git_ref('heads/main')
# master_sha = master_ref.object.sha
# base_tree = repo.get_git_tree(master_sha)

# element_list = list()
# for i, entry in enumerate(file_list):
#     with open(entry) as input_file:
#         data = input_file.read()
#     if entry.endswith('.png'): # images must be encoded
#         data = base64.b64encode(data)
#     element = InputGitTreeElement(file_names[i], '100644', 'blob', data)
#     element_list.append(element)

# tree = repo.create_git_tree(element_list, base_tree)
# parent = repo.get_git_commit(master_sha)
# commit = repo.create_git_commit(commit_message, tree, [parent])
# master_ref.edit(commit.sha)










