import streamlit as st
import pandas as pd
import os

# Set page layout to be wider for easier reading
st.set_page_config(layout="wide", page_title="Summary Annotator")

FILE_PATH = "finalDataset.csv"

# --- 1. Load Data & Initialize State ---
# We use session_state so the app remembers which row you are on
if 'current_index' not in st.session_state:
    st.session_state.current_index = 0

if 'df' not in st.session_state:
    if os.path.exists(FILE_PATH):
        # Try standard UTF-8 first
        try:
            st.session_state.df = pd.read_csv(FILE_PATH, encoding='utf-8')
        except UnicodeDecodeError:
            # Fallback to the most common encodings for scraped/Excel data
            try:
                st.session_state.df = pd.read_csv(FILE_PATH, encoding='latin-1')
            except UnicodeDecodeError:
                st.session_state.df = pd.read_csv(FILE_PATH, encoding='cp1252')
    else:
        st.error(f"Cannot find {FILE_PATH}. Make sure it is in the same folder as this script.")
        st.stop()

df = st.session_state.df
total_rows = len(df)

st.title("📝 Gold Standard Summary Annotator")

# --- 2. Progress Bar & Layout ---
st.write(f"**Row:** {st.session_state.current_index + 1} of {total_rows}")
st.progress(st.session_state.current_index / total_rows if total_rows > 0 else 0)

if st.session_state.current_index < total_rows:
    current_row = df.iloc[st.session_state.current_index]

    # --- 3. Toggle for Extra Columns ---
    show_extras = st.toggle("Show extra columns (extractive_bullets, topic_label, etc.)")

    # --- 4. Display Texts ---
    # Put Source Text and Extra Info in a visually distinct container
    with st.container(border=True):
        st.subheader("Source Text")

        # Make the source text area scrollable (300 pixels high)
        # and remove the blockquote so Streamlit renders tables properly!
        with st.container(height=300, border=False):
            st.markdown(current_row.get('source_text', 'No source text found'))

        if show_extras:
            st.divider()
            st.subheader("Additional Columns")
            extra_cols = [col for col in df.columns if col not in ['source_text', 'abstractive_summary']]
            for col in extra_cols:
                st.markdown(f"**{col}:** {current_row.get(col, '')}")

    st.write("---")
    
    # --- 5. Edit Summary ---
    st.subheader("AI Abstractive Summary (Edit below)")
    current_summary = current_row.get('abstractive_summary', '')
    
    # Text area for user to make edits
    edited_summary = st.text_area(
        "Make your touch-ups here:", 
        value=str(current_summary), 
        height=150, 
        label_visibility="collapsed"
    )

    # --- 6. Navigation & Saving ---
    col1, col2, col3 = st.columns([1, 1, 4])
    
    with col1:
        # Previous Button
        if st.button("⬅️ Previous Row", use_container_width=True):
            if st.session_state.current_index > 0:
                st.session_state.current_index -= 1
                st.rerun()

    with col2:
        # Save & Next Button
        if st.button("Save & Next ➡️", type="primary", use_container_width=True):
            # 1. Update dataframe in memory
            df.at[st.session_state.current_index, 'abstractive_summary'] = edited_summary
            
            # 2. Save directly back to the original CSV
            df.to_csv(FILE_PATH, index=False)
            
            # 3. Update session state
            st.session_state.df = df
            
            # 4. Move to next row
            if st.session_state.current_index < total_rows - 1:
                st.session_state.current_index += 1
            st.rerun()

else:
    st.success("🎉 You have reached the end of the dataset!")
    if st.button("Start Over"):
        st.session_state.current_index = 0
        st.rerun()