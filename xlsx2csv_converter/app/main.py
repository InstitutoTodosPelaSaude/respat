from typing import Union
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import FileResponse
from pathlib import Path
from .converter import Converter
from .utils import concat_csv_files

app = FastAPI()

# Create Paths to store uploaded and converted files
UPLOADED_FILES_PATH = "uploaded_files"
CONVERTED_FILES_PATH = "converted_files"

# Create Paths if they don't exist
Path(UPLOADED_FILES_PATH).mkdir(parents=True, exist_ok=True)
Path(CONVERTED_FILES_PATH).mkdir(parents=True, exist_ok=True)

# Create a Converter instance
converter = Converter(CONVERTED_FILES_PATH)

@app.get("/")
async def read_root():
    return {"Hello": "World"}

@app.post("/convert/")
async def convert(file: UploadFile = File(...)):
    # Save File
    try:
        contents = file.file.read()
        with open(f'{UPLOADED_FILES_PATH}/{file.filename}', "wb") as f:
            f.write(contents)
    except Exception as e:
        return {"error": str(e)}
    finally:
        file.file.close()

    # Convert File
    try:
        converted_folder = converter.convert(f'{UPLOADED_FILES_PATH}/{file.filename}')
    except Exception as e:
        return {"error": str(e)}
    
    # Concat files
    concat_csv_files(converted_folder, f'{CONVERTED_FILES_PATH}/{file.filename}.csv')

    # Delete files
    for file_ in Path(converted_folder).glob('*.csv'):
        file_.unlink()
    Path(converted_folder).rmdir()
    Path(f'{UPLOADED_FILES_PATH}/{file.filename}').unlink()

    # Return converted file
    return FileResponse(f'{CONVERTED_FILES_PATH}/{file.filename}.csv', media_type='text/csv', filename=f'{file.filename}.csv')

    
