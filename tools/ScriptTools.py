import re


def clean_comments_and_whitespace(source_code_text : str) -> str:
    # Combine all lines into a single string for easier processing
    source_code = "\n".join(source_code_text)

    # Remove single-line comments (--)
    source_code = re.sub(r"--.*", "", source_code)

    # Remove multiline comments (/* ... */)
    source_code = re.sub(r"/\*.*?\*/", "", source_code, flags=re.DOTALL)

    # Normalize whitespace: replace multiple spaces with a single space
    source_code = re.sub(r"\s{2,}", " ", source_code)
    return source_code