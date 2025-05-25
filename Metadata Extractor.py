import streamlit as st

import re

import json

import yaml



# === Page Setup ===

st.set_page_config(page_title="Automated Metadata Extractor", layout="wide")

st.title("Automated Metadata Extractor")



# === Login Handling ===

PASSWORD = "icsmde2025"

if "authenticated" not in st.session_state:

    st.session_state.authenticated = False



if not st.session_state.authenticated:

    with st.form("login_form"):

        password = st.text_input("Enter Password:", type="password")

        login_btn = st.form_submit_button("Login")

        if login_btn:

            if password == PASSWORD:

                st.session_state.authenticated = True

                st.success("Login successful.")

            else:

                st.error("Incorrect password.")

    st.stop()



# === Column Extractor ===

def extract_columns(select_block):

    expressions = [col.strip() for col in select_block.split(',')]

    columns = []

    for expr in expressions:

        if " as " in expr.lower():

            parts = re.split(r'\s+as\s+', expr, flags=re.IGNORECASE)

            columns.append({"expression": parts[0], "alias": parts[1]})

        else:

            columns.append({"expression": expr, "alias": expr.split('.')[-1]})

    return columns



# === Main Metadata Parser ===

def traditional_parse(content, file_extension):

    metadata = {

        "source_schema": [],

        "business_logic": [],

        "target_schema": [],

        "schedule": [],

        "execution_condition": {}

    }



    # SQL / BTEQ

    if file_extension in ['.sql', '.bteq']:

        tables = set(re.findall(r'\bFROM\s+([a-zA-Z0-9_.]+)', content, re.IGNORECASE))

        tables.update(re.findall(r'\bJOIN\s+([a-zA-Z0-9_.]+)', content, re.IGNORECASE))

        for t in tables:

            metadata["source_schema"].append({"table": t, "columns": []})



        select_blocks = re.findall(r'\bSELECT\s+(.*?)\s+FROM\b', content, re.IGNORECASE | re.DOTALL)

        for block in select_blocks:

            metadata["business_logic"].append({"columns": extract_columns(block)})



        targets = re.findall(r'\bINSERT\s+INTO\s+([a-zA-Z0-9_.]+)', content, re.IGNORECASE)

        targets += re.findall(r'\.EXPORT\s+FILE\s*=\s*["\']?(\/?[a-zA-Z0-9_\-/\.]+)["\']?', content, re.IGNORECASE)

        for t in targets:

            if "gs://" in t:

                metadata["target_schema"].append({"type": "GCS", "destination": t})

            elif "/sftp" in t or "/ftp" in t or "/mnt/" in t:

                metadata["target_schema"].append({"type": "SFTP", "destination": t})

            else:

                metadata["target_schema"].append({"table": t, "columns": []})



    # DTSX

    elif file_extension == '.dtsx':

        sources = re.findall(r'<DTS:Connection.*?ObjectName="([^"]+)"', content)

        targets = re.findall(r'<DTS:Destination.*?FileName="([^"]+)"', content)

        metadata["source_schema"] = [{"table": s, "columns": []} for s in sources]

        for t in targets:

            if "gs://" in t:

                metadata["target_schema"].append({"type": "GCS", "destination": t})

            elif "/sftp" in t or "/ftp" in t:

                metadata["target_schema"].append({"type": "SFTP", "destination": t})

            else:

                metadata["target_schema"].append({"destination": t})



    # Python / Java / C# / Shell

    elif file_extension in ['.py', '.java', '.cs', '.sh']:

        queries = re.findall(r'SELECT\s+.*?FROM\s+.*?(?:;|\n|$)', content, re.IGNORECASE | re.DOTALL)

        queries += re.findall(r'"""(SELECT .*?)"""', content, re.IGNORECASE | re.DOTALL)

        queries += re.findall(r"'''(SELECT .*?)'''", content, re.IGNORECASE | re.DOTALL)

        queries = [q.replace('\n', ' ').strip() for q in queries]



        for q in queries:

            tables = re.findall(r'\bFROM\s+([a-zA-Z0-9_.]+)', q, re.IGNORECASE)

            for t in tables:

                metadata["source_schema"].append({"table": t, "columns": []})

            select_blocks = re.findall(r'SELECT\s+(.*?)\s+FROM', q, re.IGNORECASE | re.DOTALL)

            for block in select_blocks:

                metadata["business_logic"].append({"columns": extract_columns(block)})



        targets = re.findall(r'(https?://[^\s"\']+)', content)

        targets += re.findall(r'\.to_json\s*\(\s*["\'](.*?)["\']', content)

        for t in targets:

            if "gs://" in t:

                metadata["target_schema"].append({"type": "GCS", "destination": t})

            elif t.startswith("http"):

                metadata["target_schema"].append({"type": "API", "destination": t})

            elif "/sftp" in t or "/ftp" in t or "/mnt/" in t:

                metadata["target_schema"].append({"type": "SFTP", "destination": t})

            else:

                metadata["target_schema"].append({"destination": t})



    # Schedule

    schedule_matches = re.findall(

        r'(Daily|Hourly|cron\(.+?\)|every\s+\d+\s+(minutes|hours|days)|@Scheduled\(cron\s*=\s*"[^"]+"\))',

        content, re.IGNORECASE)

    metadata["schedule"] = list(set([" ".join(sched).strip() for sched in schedule_matches if isinstance(sched, tuple)]))



    # Execution condition based on primary source table

    if metadata["source_schema"]:

        metadata["execution_condition"] = {

            "type": "table_update",

            "table": metadata["source_schema"][0]["table"]

        }



    return metadata



# === Streamlit Upload UI ===

with st.form("metadata_form"):

    uploaded_file = st.file_uploader("Upload an egress job script", type=["sql", "bteq", "dtsx", "cs", "java", "py", "sh"])

    submit_button = st.form_submit_button("Submit")



# === On Submit, Parse and Show Metadata ===

if submit_button:

    if uploaded_file:

        file_extension = '.' + uploaded_file.name.split('.')[-1].lower()

        base_filename = uploaded_file.name.rsplit('.', 1)[0]

        script_content = uploaded_file.read().decode("utf-8")



        metadata = traditional_parse(script_content, file_extension)



        st.subheader("Extracted Metadata")

        st.json(metadata)



        # Export buttons

        json_str = json.dumps(metadata, indent=4)

        st.download_button("Download JSON", data=json_str, file_name=f"{base_filename}.json", mime="application/json")



        yaml_str = yaml.dump(metadata, sort_keys=False)

        st.download_button("Download YAML", data=yaml_str, file_name=f"{base_filename}.yaml", mime="text/yaml")

    else:

        st.warning("Please upload a valid script file.")
