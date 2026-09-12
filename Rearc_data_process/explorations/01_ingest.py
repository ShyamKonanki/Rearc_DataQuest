# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ingest raw data (v1)
# MAGIC
# MAGIC Pull the BLS `pr` productivity files and the DataUSA population API into our raw Volume.
# MAGIC
# MAGIC Notes to self:
# MAGIC - BLS returns 403 unless we send a User-Agent with a contact email.
# MAGIC - Re-running should skip files that haven't changed on the server.

# COMMAND ----------

# Config
# Widgets for the values that change per environment (so DAB can pass them in later).
dbutils.widgets.text("catalog", "rearc_dev")
dbutils.widgets.text("raw_schema", "dataquest_raw")
dbutils.widgets.text("volume", "landing")
dbutils.widgets.text("contact_email", "shyamk120694@gmail.com")   # <-- put a real email here

catalog = dbutils.widgets.get("catalog")
raw_schema = dbutils.widgets.get("raw_schema")
volume = dbutils.widgets.get("volume")
contact_email = dbutils.widgets.get("contact_email")

bls_url = "https://download.bls.gov/pub/time.series/pr/"
pop_url = ("https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
           "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population")

raw_path = f"/Volumes/{catalog}/{raw_schema}/{volume}"
bls_path = f"{raw_path}/bls/pr"
pop_path = f"{raw_path}/population"
manifest_file = f"{raw_path}/_manifest.json"

dbutils.fs.mkdirs(bls_path)
dbutils.fs.mkdirs(pop_path)

print("Raw path:", raw_path)

# COMMAND ----------

import requests
import re
import json
import os

# BLS wants contact info in the request, otherwise we get a 403
headers = {"User-Agent": f"{contact_email}"}

# COMMAND ----------

resp = requests.get(bls_url, headers=headers)
resp.text

# COMMAND ----------

# Get the list of files from the BLS folder page
resp = requests.get(bls_url, headers=headers)
resp.raise_for_status()

# The page is a simple HTML listing. Grab the file names from the links.
file_names = re.findall(r'HREF="[^"]*/pr\.([^"/]+)"', resp.text)
file_names = sorted(set("pr." + f for f in file_names))

print(f"Found {len(file_names)} files:")
for f in file_names:
    print(" ", f)

# COMMAND ----------

file_names

# COMMAND ----------

# Load the manifest so we know what we already downloaded last time
if os.path.exists(manifest_file):
    with open(manifest_file) as f:
        manifest = json.load(f)
else:
    manifest = {}

# COMMAND ----------

manifest

# COMMAND ----------

file_names[:2]

# COMMAND ----------

# Download each file, but skip it if the server's Last-Modified matches what we have
for name in file_names:
    url = bls_url + name
    dest = f"{bls_path}/{name}"

    # Check the header first so we don't re-download unchanged files
    head = requests.head(url, headers=headers)
    last_modified = head.headers.get("Last-Modified")

    if manifest.get(name) == last_modified and os.path.exists(dest):
        print("skip  ", name)
        continue

    r = requests.get(url, headers=headers)
    r.raise_for_status()
    with open(dest, "wb") as f:
        f.write(r.content)

    manifest[name] = last_modified
    print("saved ", name)

# COMMAND ----------

manifest

# COMMAND ----------

# Remove any files we had before that are no longer on the server
server_files = set(file_names)
for name in list(manifest.keys()):
    if name.startswith("pr.") and name not in server_files:
        old = f"{bls_path}/{name}"
        if os.path.exists(old):
            os.remove(old)
        del manifest[name]
        print("removed", name)

# COMMAND ----------

# Population API -> save as JSON
r = requests.get(pop_url)
r.raise_for_status()

data = r.json()
print("population records:", len(data["data"]))

with open(f"{pop_path}/population.json", "wb") as f:
    f.write(r.content)

# COMMAND ----------

# Save the manifest for next run
with open(manifest_file, "w") as f:
    json.dump(manifest, f, indent=2)

print("done")

# COMMAND ----------

# Quick check - list what we have
for f in dbutils.fs.ls(bls_path):
    print(f.name)
print("---")
for f in dbutils.fs.ls(pop_path):
    print(f.name)

# COMMAND ----------

