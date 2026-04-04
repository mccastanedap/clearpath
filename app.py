import streamlit as st
import boto3
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

st.set_page_config(
    page_title="Clearpath — Upload Your Sales Data",
    page_icon="📊",
    layout="centered"
)

st.markdown("""
    <style>
        .main { max-width: 680px; margin: 0 auto; }
        .upload-box { border: 2px dashed #ccc; border-radius: 12px; padding: 2rem; text-align: center; }
        .success-box { background: #f0fdf4; border: 1px solid #86efac; border-radius: 8px; padding: 1rem; }
        .history-row { background: #f9f9f9; border-radius: 6px; padding: 0.5rem 1rem; margin-bottom: 0.5rem; }
    </style>
""", unsafe_allow_html=True)


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )


def upload_to_s3(file_bytes, filename: str, client_name: str) -> str:
    s3 = get_s3_client()
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    key = f"uploads/{client_name}/{timestamp}_{filename}"
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=key,
        Body=file_bytes,
        Metadata={"client": client_name, "uploaded_at": timestamp},
    )
    return key


def get_upload_history(client_name: str) -> list[dict]:
    s3 = get_s3_client()
    prefix = f"uploads/{client_name}/"
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
    objects = response.get("Contents", [])
    history = []
    for obj in sorted(objects, key=lambda x: x["LastModified"], reverse=True):
        history.append({
            "file": obj["Key"].split("/")[-1],
            "uploaded_at": obj["LastModified"].strftime("%b %d, %Y at %I:%M %p"),
            "size_kb": round(obj["Size"] / 1024, 1),
        })
    return history


# ── Header ──────────────────────────────────────────────────────────────────

st.image("Clearpath_Logo.png", width=120)
st.markdown("## Upload Your Weekly Sales Data")
st.markdown(
    "Upload your sales CSV and we'll process it automatically. "
    "You'll receive your insights report by email within a few minutes."
)
st.divider()

# ── Client name input ────────────────────────────────────────────────────────

client_name = st.text_input(
    "Your business name",
    placeholder="e.g. Juice Bar NYC",
    help="This is how we'll identify your report.",
)

# ── File upload ──────────────────────────────────────────────────────────────

uploaded_file = st.file_uploader(
    "Upload your sales CSV",
    type=["csv"],
    help="Must be a .csv file exported from your POS system.",
)

if uploaded_file and not client_name:
    st.warning("Please enter your business name before uploading.")

if uploaded_file and client_name:
    preview_df = pd.read_csv(uploaded_file)
    st.markdown("#### Preview (first 5 rows)")
    st.dataframe(preview_df.head(5), use_container_width=True)

    st.markdown(f"**{len(preview_df)} rows detected** · {round(uploaded_file.size / 1024, 1)} KB")

    if st.button("Upload and run pipeline", type="primary", use_container_width=True):
        with st.spinner("Uploading to S3..."):
            uploaded_file.seek(0)
            key = upload_to_s3(uploaded_file.read(), uploaded_file.name, client_name)

        st.success(
            f"File uploaded successfully. Your insights report will arrive by email shortly.",
            icon="✅",
        )
        st.code(f"S3 path: s3://{S3_BUCKET_NAME}/{key}", language="text")

# ── Upload history ───────────────────────────────────────────────────────────

if client_name:
    st.divider()
    st.markdown("#### Upload history")
    with st.spinner("Loading history..."):
        history = get_upload_history(client_name)

    if not history:
        st.info("No uploads yet for this business.")
    else:
        for row in history:
            st.markdown(
                f"**{row['file']}** &nbsp;·&nbsp; {row['uploaded_at']} &nbsp;·&nbsp; {row['size_kb']} KB",
                unsafe_allow_html=True,
            )

# ── Footer ───────────────────────────────────────────────────────────────────

st.divider()
st.caption("Clearpath · AI-powered retail analytics · Questions? Reply to your weekly report email.")