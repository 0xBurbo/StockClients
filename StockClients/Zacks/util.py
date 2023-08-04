import uuid

def create_multipart_formdata(fields):
    boundary = str(uuid.uuid4())
    body = ""

    for field in fields:
        key = field[0]
        value = field[1]
        body += f"--{boundary}\r\nContent-Disposition: form-data; name=\"{key}\"\r\n\r\n{value}\r\n"

    body += f"--{boundary}--\r\n"
    content_type = f"multipart/form-data; boundary={boundary}"

    return content_type, body